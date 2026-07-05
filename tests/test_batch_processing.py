"""
Tests for batch repository processing features.
"""

import asyncio
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from dev_health_ops.connectors import BatchResult
from dev_health_ops.connectors.utils import match_repo_pattern
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.models.git import (
    GitCommit,
    GitCommitStat,
    Repo,
    get_repo_uuid_from_repo,
)
from dev_health_ops.processors import github as _github_processor
from dev_health_ops.processors import gitlab as _gitlab_processor

# Create namespace to match existing code references
processors = SimpleNamespace(github=_github_processor, gitlab=_gitlab_processor)


def _enable_connector_stubs(monkeypatch) -> None:
    from dev_health_ops.connectors.utils import RateLimitConfig, RateLimitGate

    monkeypatch.setattr(processors.github, "CONNECTORS_AVAILABLE", True)
    monkeypatch.setattr(processors.gitlab, "CONNECTORS_AVAILABLE", True)
    monkeypatch.setattr(processors.github, "RateLimitConfig", RateLimitConfig)
    monkeypatch.setattr(processors.github, "RateLimitGate", RateLimitGate)
    monkeypatch.setattr(processors.gitlab, "RateLimitConfig", RateLimitConfig)
    monkeypatch.setattr(processors.gitlab, "RateLimitGate", RateLimitGate)


class _GitLabProjectDiscoveryClient:
    def __init__(self, projects):
        self._projects = projects

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def list_projects(self, **kwargs):
        return self._projects

    def drain_usage_observations(self):
        return [{"route_family": "project"}]


def _stub_gitlab_project_discovery(monkeypatch, projects) -> None:
    monkeypatch.setattr(
        processors.gitlab,
        "_gitlab_code_client_from_connector",
        lambda connector: _GitLabProjectDiscoveryClient(projects),
    )


def _github_commits_async_from_sync(fake_fetch):
    async def _wrapped(
        connector,
        owner,
        repo_name,
        repo_id,
        max_commits,
        since=None,
        until=None,
        usage_sink=None,
    ):
        result = fake_fetch(None, max_commits, repo_id, since)
        if len(result) == 2:
            raw_commits, commit_objects = result
            return raw_commits, commit_objects, False
        return result

    return _wrapped


def _github_commit_stats_async_from_sync(fake_fetch):
    async def _wrapped(
        connector,
        owner,
        repo_name,
        raw_commits,
        repo_id,
        max_stats,
        since=None,
        usage_sink=None,
    ):
        return fake_fetch(raw_commits, repo_id, max_stats, since)

    return _wrapped


class _EmptyGithubPrCodeClient:
    def __init__(self, repos=None):
        self._repos = list(repos or [])
        self.list_repository_calls = []

    async def list_repositories(self, **kwargs):
        self.list_repository_calls.append(kwargs)
        return list(self._repos)

    async def iter_pulls(self, owner, repo, *, state, sort, direction, since=None):
        return []

    async def get_pull_detail(self, owner, repo, number):
        raise AssertionError(f"unexpected pull detail request for {number}")

    def drain_usage_observations(self):
        return []

    async def close(self):
        return None


def _repo_client_for(*repos):
    return _EmptyGithubPrCodeClient(repos)


@pytest.mark.asyncio
async def test_process_github_repos_batch_uses_pattern_owner_for_repo_discovery(
    monkeypatch,
):
    _enable_connector_stubs(monkeypatch)

    inserted_repos = []

    class DummyStore:
        async def insert_repo(self, repo):
            inserted_repos.append(repo)

    class DummyConnector:
        def __init__(self, token: str):
            self.token = token

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

    repo = SimpleNamespace(
        id=1,
        name="service",
        full_name="org/service",
        default_branch="main",
        description=None,
        url="https://github.com/org/service",
        created_at=None,
        updated_at=None,
        language="Python",
        stars=0,
        forks=0,
    )
    code_client = _repo_client_for(repo)

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: code_client,
    )

    await processors.github.process_github_repos_batch(
        store=DummyStore(),
        token="test_token",
        pattern="org/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        sync_git=False,
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=False,
        sync_incidents=False,
        sync_security=False,
    )

    assert len(inserted_repos) == 1
    assert code_client.list_repository_calls == [
        {
            "org_name": None,
            "user_name": "org",
            "pattern": "org/*",
            "max_repos": None,
        }
    ]


@pytest.mark.asyncio
async def test_process_gitlab_projects_batch_upserts_during_sync_processing(
    monkeypatch,
):
    """Default (sync) batch mode should still upsert before processing ends."""
    _enable_connector_stubs(monkeypatch)

    monkeypatch.setattr(
        processors.gitlab,
        "_fetch_gitlab_commits_sync",
        lambda *args, **kwargs: ([], []),
    )
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_commit_stats_sync", lambda *args, **kwargs: []
    )

    inserted = threading.Event()

    class DummyStore:
        async def insert_repo(self, repo):
            inserted.set()

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            return

        async def insert_git_pull_requests(self, pr_data):
            return

    store = DummyStore()

    project = Mock()
    project.id = 123
    project.full_name = "group/proj"
    project.url = "https://example.com/group/proj"
    project.default_branch = "main"

    result = BatchResult(repository=project, stats=None, success=True)
    _stub_gitlab_project_discovery(monkeypatch, [project])

    class DummyConnector:
        def __init__(self, url: str, private_token: str):
            self.url = url
            self.private_token = private_token
            self.rest_client = Mock(get_merge_requests=lambda **kwargs: [])
            self.gitlab = Mock()
            self.gitlab.projects = Mock(get=lambda project_id: None)

        def get_projects_with_stats(self, **kwargs):
            on_project_complete = kwargs.get("on_project_complete")
            if on_project_complete:
                on_project_complete(result)

            # Give the event loop time to upsert while we're still running.
            deadline = time.time() + 2
            while time.time() < deadline and not inserted.is_set():
                time.sleep(0.01)

            assert inserted.is_set(), "Expected upsert during sync processing"
            return [result]

        def close(self):
            return

    monkeypatch.setattr(processors.gitlab, "GitLabConnector", DummyConnector)

    await processors.gitlab.process_gitlab_projects_batch(
        store=store,
        token="test_token",
        gitlab_url="https://gitlab.com",
        group_name="group",
        pattern="group/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=False,
    )


@pytest.mark.asyncio
async def test_process_gitlab_projects_batch_persists_instance_discriminator(
    monkeypatch,
):
    """[CHAOS-2801] process_gitlab_projects_batch must persist the batch's
    configured base URL (this call's own ``gitlab_url`` argument) as
    ``settings.gitlab_instance_url`` on every written ``Repo`` row — same
    discriminator, same shared ``normalize_gitlab_instance``, as the
    single-project write site (process_gitlab_project), so
    job_work_items.py's numeric-id scoping can reject a same-``project_id``
    row discovered from a DIFFERENT GitLab instance. The input below carries
    an explicit default :443 port and a trailing slash that must be
    normalized away at persist time (codex MED PR #1148)."""
    _enable_connector_stubs(monkeypatch)

    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_commits_sync", lambda *a, **k: ([], [])
    )
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_commit_stats_sync", lambda *a, **k: []
    )

    inserted_repos: list[Repo] = []
    inserted = threading.Event()

    class DummyStore:
        async def insert_repo(self, repo: Repo) -> None:
            inserted_repos.append(repo)
            inserted.set()

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            return

        async def insert_git_pull_requests(self, pr_data):
            return

    store = DummyStore()

    project = Mock()
    project.id = 456
    project.full_name = "group/proj"
    project.url = "https://example.com/group/proj"
    project.default_branch = "main"

    result = BatchResult(repository=project, stats=None, success=True)
    _stub_gitlab_project_discovery(monkeypatch, [project])

    class DummyConnector:
        def __init__(self, url: str, private_token: str):
            self.url = url
            self.private_token = private_token
            self.rest_client = Mock(get_merge_requests=lambda **kwargs: [])
            self.gitlab = Mock()
            self.gitlab.projects = Mock(get=lambda project_id: None)

        def get_projects_with_stats(self, **kwargs):
            on_project_complete = kwargs.get("on_project_complete")
            if on_project_complete:
                on_project_complete(result)

            # Runs in the executor's worker thread (this whole method does,
            # since ``use_async=False``): give the main loop's thread time to
            # process the call_soon_threadsafe-scheduled enqueue and run
            # store_result before this method returns and unblocks
            # ``results_queue.join()`` — mirrors the sibling upsert-during-
            # sync-processing test above, same race.
            deadline = time.time() + 2
            while time.time() < deadline and not inserted.is_set():
                time.sleep(0.01)
            assert inserted.is_set(), "Expected upsert during sync processing"
            return [result]

        def close(self):
            return

    monkeypatch.setattr(processors.gitlab, "GitLabConnector", DummyConnector)

    await processors.gitlab.process_gitlab_projects_batch(
        store=store,
        token="test_token",
        gitlab_url="https://gitlab-self-hosted.example.com:443/",
        group_name="group",
        pattern="group/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=False,
    )

    assert len(inserted_repos) == 1
    assert (
        inserted_repos[0].settings["gitlab_instance_url"]
        == "https://gitlab-self-hosted.example.com"
    )
    # The per-project url (BatchResult.repository.url) stays untouched.
    assert inserted_repos[0].settings["url"] == "https://example.com/group/proj"
    assert inserted_repos[0].settings["project_id"] == 456


@pytest.mark.asyncio
async def test_process_github_repos_batch_upserts_during_async_processing(monkeypatch):
    """Ensure async batch mode upserts as repos complete."""
    _enable_connector_stubs(monkeypatch)

    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(lambda *args, **kwargs: ([], [])),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commit_stats_async",
        _github_commit_stats_async_from_sync(lambda *args, **kwargs: []),
    )

    # A store stub that records when insert_repo is called.
    inserted_event = asyncio.Event()

    class DummyStore:
        async def insert_repo(self, repo):
            inserted_event.set()

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            return

        async def insert_git_pull_requests(self, pr_data):
            return

    store = DummyStore()

    repo = Mock()
    repo.id = 123
    repo.full_name = "org/repo"
    repo.url = "https://example.com/org/repo"
    repo.default_branch = "main"
    repo.language = "Python"

    stats = Mock()
    stats.total_commits = 1
    stats.additions = 2
    stats.deletions = 1

    result = BatchResult(repository=repo, stats=stats, success=True)

    class DummyRepo:
        """Mock GitHub repository object."""

        def get_pulls(self, state="all"):
            """Return empty iterator for PRs."""
            return iter([])

    class DummyGithub:
        """Mock PyGithub Github object."""

        def get_repo(self, full_name: str):
            """Return a dummy repo."""
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.token = token
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)

            # Wait for the consumer to store the result.
            await asyncio.wait_for(inserted_event.wait(), timeout=2)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )

    await processors.github.process_github_repos_batch(
        store=store,
        token="test_token",
        org_name="org",
        pattern="org/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
    )


@pytest.mark.asyncio
async def test_process_github_repos_batch_stores_commits_and_stats(monkeypatch):
    """Batch GitHub processing should persist commits and stats for metrics."""
    # Force connectors availability and stub API helpers.
    _enable_connector_stubs(monkeypatch)

    recorded_commits = []
    recorded_stats = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            recorded_commits.extend(commit_data)

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    repo = Mock()
    repo.id = 321
    repo.full_name = "org/repo-metrics"
    repo.url = "https://example.com/org/repo-metrics"
    repo.default_branch = "main"
    repo.language = "Python"

    stats = Mock()
    stats.total_commits = 1
    stats.additions = 2
    stats.deletions = 1

    result = BatchResult(repository=repo, stats=stats, success=True)

    def fake_fetch_commits(gh_repo, max_commits, repo_id, since=None):
        commit = GitCommit(
            repo_id=repo_id,
            hash="abc123",
            message="msg",
            author_name="alice",
            author_email=None,
            author_when=datetime.now(timezone.utc),
            committer_name="alice",
            committer_email=None,
            committer_when=datetime.now(timezone.utc),
            parents=1,
        )
        return ["raw"], [commit]

    def fake_fetch_commit_stats(raw_commits, repo_id, max_stats, since=None, gate=None):
        return [
            GitCommitStat(
                repo_id=repo_id,
                commit_hash="abc123",
                file_path="file.txt",
                additions=1,
                deletions=0,
                old_file_mode="unknown",
                new_file_mode="unknown",
            )
        ]

    class DummyRepo:
        def get_pulls(self, state="all"):
            return iter([])

    class DummyGithub:
        def get_repo(self, full_name: str):
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _EmptyGithubPrCodeClient(),
    )
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(fake_fetch_commits),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commit_stats_async",
        _github_commit_stats_async_from_sync(fake_fetch_commit_stats),
    )

    store = DummyStore()

    await processors.github.process_github_repos_batch(
        store=store,
        token="test_token",
        org_name="org",
        pattern="org/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
    )

    assert any(c.hash == "abc123" for c in recorded_commits)
    expected_repo_id = get_repo_uuid_from_repo(repo.full_name)
    assert all(c.repo_id == expected_repo_id for c in recorded_commits)
    assert "abc123" in {s.commit_hash for s in recorded_stats}


@pytest.mark.asyncio
async def test_process_github_repos_batch_over_cap_window_skips_stats(monkeypatch):
    """Windowed (since-bound) batch sync over the hard cap must skip per-commit
    detail fetch and persist zero commit stats rather than a partial day.

    Regression: the batch path bypassed ``_sync_github_commit_stats`` and wrote
    partial commit stats for since-bound over-cap windows.
    """
    _enable_connector_stubs(monkeypatch)

    monkeypatch.setenv("COMMIT_STATS_MAX_COMMITS", "300")
    since = datetime(2026, 5, 13, tzinfo=timezone.utc)

    recorded_stats = []
    stats_fetch_calls = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    repo = Mock()
    repo.id = 999
    repo.full_name = "org/over-cap"
    repo.url = "https://example.com/org/over-cap"
    repo.default_branch = "main"
    repo.language = "Python"

    stats = Mock()
    stats.total_commits = 301
    stats.additions = 999
    stats.deletions = 111
    result = BatchResult(repository=repo, stats=stats, success=True)

    raw_commits = [SimpleNamespace(sha=f"sha-{i}") for i in range(301)]

    def fake_fetch_commits(gh_repo, max_commits, repo_id, since=None):
        return raw_commits, []

    def fake_fetch_commit_stats(*args, **kwargs):
        stats_fetch_calls.append(args)
        return []

    class DummyRepo:
        def get_pulls(self, state="all"):
            return iter([])

    class DummyGithub:
        def get_repo(self, full_name: str):
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(fake_fetch_commits),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commit_stats_async",
        _github_commit_stats_async_from_sync(fake_fetch_commit_stats),
    )

    store = DummyStore()

    await processors.github.process_github_repos_batch(
        store=store,
        token="test_token",
        org_name="org",
        pattern="org/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
        since=since,
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=False,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
        backfill_missing=False,
    )

    assert stats_fetch_calls == []
    assert recorded_stats == []


@pytest.mark.asyncio
async def test_process_github_repos_batch_truncated_window_skips_stats(monkeypatch):
    """A since-bounded window that hits ``max_commits_per_repo`` AND has more
    commits beyond it (the fetch reports ``window_truncated=True``) must skip
    per-file stats rather than persist a partial day.
    """
    _enable_connector_stubs(monkeypatch)

    since = datetime(2026, 5, 13, tzinfo=timezone.utc)
    recorded_stats = []
    stats_fetch_calls = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    repo = Mock()
    repo.id = 1000
    repo.full_name = "org/exact-cap"
    repo.url = "https://example.com/org/exact-cap"
    repo.default_branch = "main"
    repo.language = "Python"

    stats = Mock()
    stats.total_commits = 2
    stats.additions = 999
    stats.deletions = 111
    result = BatchResult(repository=repo, stats=stats, success=True)

    raw_commits = [SimpleNamespace(sha="sha-0"), SimpleNamespace(sha="sha-1")]

    def fake_fetch_commits(gh_repo, max_commits, repo_id, since=None):
        assert max_commits == 2
        # window_truncated=True: cap hit and at least one more commit existed in
        # the window beyond it, so the fetched stats would be a partial day.
        return raw_commits, [], True

    def fake_fetch_commit_stats(*args, **kwargs):
        stats_fetch_calls.append(args)
        return []

    class DummyRepo:
        def get_pulls(self, state="all"):
            return iter([])

    class DummyGithub:
        def get_repo(self, full_name: str):
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(fake_fetch_commits),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commit_stats_async",
        _github_commit_stats_async_from_sync(fake_fetch_commit_stats),
    )

    await processors.github.process_github_repos_batch(
        store=DummyStore(),
        token="test_token",
        org_name="org",
        pattern="org/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
        since=since,
        max_commits_per_repo=2,
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=False,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
        backfill_missing=False,
    )

    assert stats_fetch_calls == []
    assert recorded_stats == []


@pytest.mark.asyncio
async def test_process_github_repos_batch_undersized_window_writes_stats(monkeypatch):
    """Regression: a windowed sync that sets ``max_commits_per_repo`` but whose
    window is *fully covered* (fewer commits than the cap) MUST still write
    commit stats. The old skip used ``len(raw_commits) >= stats_limit`` — always
    true once a cap was set — so it silently dropped stats for small windows.
    """
    _enable_connector_stubs(monkeypatch)

    since = datetime(2026, 5, 13, tzinfo=timezone.utc)
    recorded_stats = []
    stats_fetch_calls = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    repo = Mock()
    repo.id = 1001
    repo.full_name = "org/undersized"
    repo.url = "https://example.com/org/undersized"
    repo.default_branch = "main"
    repo.language = "Python"

    stats = Mock()
    stats.total_commits = 2
    stats.additions = 999
    stats.deletions = 111
    result = BatchResult(repository=repo, stats=stats, success=True)

    # 2 commits in the window, well under the max_commits_per_repo=10 cap.
    raw_commits = [SimpleNamespace(sha="sha-0"), SimpleNamespace(sha="sha-1")]

    def fake_fetch_commits(gh_repo, max_commits, repo_id, since=None):
        assert max_commits == 10
        return raw_commits, []

    def fake_fetch_commit_stats(*args, **kwargs):
        stats_fetch_calls.append(args)
        repo_id = args[1]
        return [
            GitCommitStat(
                repo_id=repo_id,
                commit_hash="sha-0",
                file_path="file.txt",
                additions=1,
                deletions=0,
                old_file_mode="unknown",
                new_file_mode="unknown",
            )
        ]

    class DummyRepo:
        def get_pulls(self, state="all"):
            return iter([])

    class DummyGithub:
        def get_repo(self, full_name: str):
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(fake_fetch_commits),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commit_stats_async",
        _github_commit_stats_async_from_sync(fake_fetch_commit_stats),
    )

    await processors.github.process_github_repos_batch(
        store=DummyStore(),
        token="test_token",
        org_name="org",
        pattern="org/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
        since=since,
        max_commits_per_repo=10,
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=False,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
        backfill_missing=False,
    )

    # Detail fetch was invoked and the real per-file stat was persisted.
    assert stats_fetch_calls != []
    assert "sha-0" in {s.commit_hash for s in recorded_stats}


@pytest.mark.asyncio
async def test_process_github_repos_batch_exact_complete_window_writes_stats(
    monkeypatch,
):
    """Codex adversarial-review regression: a since-bounded window whose commit
    count lands *exactly* on ``max_commits_per_repo`` but is COMPLETE (the fetch
    reports ``window_truncated=False`` — no commit existed beyond the cap) MUST
    write stats. Counting alone (``>= max_commits``) cannot tell this apart from a
    truncated window, so it would silently drop a complete day's stats.
    """
    _enable_connector_stubs(monkeypatch)

    since = datetime(2026, 5, 13, tzinfo=timezone.utc)
    recorded_stats = []
    stats_fetch_calls = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    repo = Mock()
    repo.id = 1002
    repo.full_name = "org/exact-complete"
    repo.url = "https://example.com/org/exact-complete"
    repo.default_branch = "main"
    repo.language = "Python"

    stats = Mock()
    stats.total_commits = 2
    stats.additions = 999
    stats.deletions = 111
    result = BatchResult(repository=repo, stats=stats, success=True)

    # Exactly max_commits_per_repo=2 commits in the window, and nothing beyond it.
    raw_commits = [SimpleNamespace(sha="sha-0"), SimpleNamespace(sha="sha-1")]

    def fake_fetch_commits(gh_repo, max_commits, repo_id, since=None):
        assert max_commits == 2
        # window_truncated=False: the iterator ended at the cap, proving the
        # window held exactly max_commits commits (complete, not truncated).
        return raw_commits, [], False

    def fake_fetch_commit_stats(*args, **kwargs):
        stats_fetch_calls.append(args)
        repo_id = args[1]
        return [
            GitCommitStat(
                repo_id=repo_id,
                commit_hash="sha-0",
                file_path="file.txt",
                additions=1,
                deletions=0,
                old_file_mode="unknown",
                new_file_mode="unknown",
            )
        ]

    class DummyRepo:
        def get_pulls(self, state="all"):
            return iter([])

    class DummyGithub:
        def get_repo(self, full_name: str):
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(fake_fetch_commits),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commit_stats_async",
        _github_commit_stats_async_from_sync(fake_fetch_commit_stats),
    )

    await processors.github.process_github_repos_batch(
        store=DummyStore(),
        token="test_token",
        org_name="org",
        pattern="org/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
        since=since,
        max_commits_per_repo=2,
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=False,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
        backfill_missing=False,
    )

    # Exact-but-complete window: stats ARE fetched and persisted.
    assert stats_fetch_calls != []
    assert "sha-0" in {s.commit_hash for s in recorded_stats}


@pytest.mark.asyncio
async def test_process_github_repos_batch_commit_stats_rate_limit_propagates(
    monkeypatch,
):
    _enable_connector_stubs(monkeypatch)

    since = datetime(2026, 5, 13, tzinfo=timezone.utc)
    recorded_stats = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    repo = Mock()
    repo.id = 1001
    repo.full_name = "org/rate-limited"
    repo.url = "https://example.com/org/rate-limited"
    repo.default_branch = "main"
    repo.language = "Python"

    stats = Mock()
    stats.total_commits = 1
    stats.additions = 999
    stats.deletions = 111
    result = BatchResult(repository=repo, stats=stats, success=True)

    def fake_fetch_commits(gh_repo, max_commits, repo_id, since=None):
        raise RateLimitException("limited", retry_after_seconds=42.0)

    class DummyRepo:
        def get_pulls(self, state="all"):
            return iter([])

    class DummyGithub:
        def get_repo(self, full_name: str):
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(fake_fetch_commits),
    )

    with pytest.raises(RateLimitException) as exc_info:
        await processors.github.process_github_repos_batch(
            store=DummyStore(),
            token="test_token",
            org_name="org",
            pattern="org/*",
            batch_size=1,
            max_concurrent=1,
            rate_limit_delay=0,
            use_async=True,
            since=since,
            sync_prs=False,
            sync_cicd=False,
            sync_deployments=False,
            sync_incidents=False,
            sync_security=False,
            sync_tests=False,
            backfill_missing=False,
        )

    assert exc_info.value.retry_after_seconds == 42.0
    assert recorded_stats == []


@pytest.mark.asyncio
async def test_process_github_repos_batch_multi_repo_rate_limit_does_not_hang(
    monkeypatch,
):
    _enable_connector_stubs(monkeypatch)

    since = datetime(2026, 5, 13, tzinfo=timezone.utc)
    recorded_stats = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    def make_result(index: int):
        repo = Mock()
        repo.id = index
        repo.full_name = f"org/rate-limited-{index}"
        repo.url = f"https://example.com/org/rate-limited-{index}"
        repo.default_branch = "main"
        repo.language = "Python"
        stats = Mock()
        stats.total_commits = 1
        stats.additions = 999
        stats.deletions = 111
        return BatchResult(repository=repo, stats=stats, success=True)

    results = [make_result(index) for index in range(3)]

    def fake_fetch_commits(gh_repo, max_commits, repo_id, since=None):
        raise RateLimitException("limited", retry_after_seconds=42.0)

    class DummyRepo:
        def get_pulls(self, state="all"):
            return iter([])

    class DummyGithub:
        def get_repo(self, full_name: str):
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                for result in results:
                    on_repo_complete(result)
            return results

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(*[result.repository for result in results]),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(fake_fetch_commits),
    )

    with pytest.raises(RateLimitException) as exc_info:
        await asyncio.wait_for(
            processors.github.process_github_repos_batch(
                store=DummyStore(),
                token="test_token",
                org_name="org",
                pattern="org/*",
                batch_size=1,
                max_concurrent=1,
                rate_limit_delay=0,
                use_async=True,
                since=since,
                sync_prs=False,
                sync_cicd=False,
                sync_deployments=False,
                sync_incidents=False,
                sync_security=False,
                sync_tests=False,
                backfill_missing=False,
            ),
            timeout=2,
        )

    assert exc_info.value.retry_after_seconds == 42.0
    assert recorded_stats == []


@pytest.mark.asyncio
async def test_process_github_repos_batch_commit_detail_rate_limit_propagates(
    monkeypatch,
):
    _enable_connector_stubs(monkeypatch)

    since = datetime(2026, 5, 13, tzinfo=timezone.utc)
    recorded_stats = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    repo = Mock()
    repo.id = 1002
    repo.full_name = "org/detail-rate-limited"
    repo.url = "https://example.com/org/detail-rate-limited"
    repo.default_branch = "main"
    repo.language = "Python"

    stats = Mock()
    stats.total_commits = 1
    stats.additions = 999
    stats.deletions = 111
    result = BatchResult(repository=repo, stats=stats, success=True)

    class RateLimitedCommit:
        sha = "sha-rate-limited"
        commit = None

        @property
        def files(self):
            raise RateLimitException("limited", retry_after_seconds=43.0)

    def fake_fetch_commits(gh_repo, max_commits, repo_id, since=None):
        return [RateLimitedCommit()], []

    class DummyRepo:
        def get_pulls(self, state="all"):
            return iter([])

    class DummyGithub:
        def get_repo(self, full_name: str):
            return DummyRepo()

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        _github_commits_async_from_sync(fake_fetch_commits),
    )

    async def fake_fetch_commit_stats(*args, **kwargs):
        raise RateLimitException("limited", retry_after_seconds=43.0)

    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commit_stats_async",
        fake_fetch_commit_stats,
    )

    with pytest.raises(RateLimitException) as exc_info:
        await processors.github.process_github_repos_batch(
            store=DummyStore(),
            token="test_token",
            org_name="org",
            pattern="org/*",
            batch_size=1,
            max_concurrent=1,
            rate_limit_delay=0,
            use_async=True,
            since=since,
            sync_prs=False,
            sync_cicd=False,
            sync_deployments=False,
            sync_incidents=False,
            sync_security=False,
            sync_tests=False,
            backfill_missing=False,
        )

    assert exc_info.value.retry_after_seconds == 43.0
    assert recorded_stats == []


@pytest.mark.asyncio
async def test_process_github_repos_batch_incident_rate_limit_propagates(
    monkeypatch,
):
    _enable_connector_stubs(monkeypatch)

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_incidents(self, incidents):
            raise AssertionError("incidents must not persist after a rate limit")

    repo = Mock()
    repo.id = 1003
    repo.full_name = "org/incident-rate-limited"
    repo.url = "https://example.com/org/incident-rate-limited"
    repo.default_branch = "main"
    repo.language = "Python"
    result = BatchResult(repository=repo, stats=None, success=True)

    class DummyGithub:
        def get_repo(self, full_name: str):
            raise AssertionError("legacy repo object must not be fetched")

    class DummyConnector:
        def __init__(self, token: str):
            self.github = DummyGithub()

        async def get_repos_with_stats_async(self, **kwargs):
            on_repo_complete = kwargs.get("on_repo_complete")
            if on_repo_complete:
                on_repo_complete(result)
            return [result]

        def get_rate_limit(self):
            return {"remaining": 0, "limit": 0}

        def close(self):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

    async def fake_fetch_incidents(*args, **kwargs):
        raise RateLimitException("limited", retry_after_seconds=44.0)

    async def fake_fetch_commits(*args, **kwargs):
        return [], [], False

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github,
        "_github_code_client_from_connector",
        lambda _connector: _repo_client_for(repo),
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_async",
        fake_fetch_commits,
    )
    monkeypatch.setattr(
        processors.github,
        "_fetch_github_incidents_async",
        fake_fetch_incidents,
    )

    with pytest.raises(RateLimitException) as exc_info:
        await processors.github.process_github_repos_batch(
            store=DummyStore(),
            token="test_token",
            org_name="org",
            pattern="org/*",
            batch_size=1,
            max_concurrent=1,
            rate_limit_delay=0,
            use_async=True,
            sync_prs=False,
            sync_cicd=False,
            sync_deployments=False,
            sync_incidents=True,
            sync_security=False,
            sync_tests=False,
            backfill_missing=False,
        )

    assert exc_info.value.retry_after_seconds == 44.0


@pytest.mark.asyncio
async def test_process_gitlab_projects_batch_stores_commits_and_stats(monkeypatch):
    """Batch GitLab processing should persist commits and stats for metrics."""
    _enable_connector_stubs(monkeypatch)

    recorded_commits = []
    recorded_stats = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            recorded_commits.extend(commit_data)

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    project = Mock()
    project.id = 456
    project.full_name = "group/proj-metrics"
    project.url = "https://example.com/group/proj-metrics"
    project.default_branch = "main"

    result = BatchResult(repository=project, stats=None, success=True)
    _stub_gitlab_project_discovery(monkeypatch, [project])

    def fake_fetch_commits(
        connector,
        project_id,
        max_commits,
        repo_id,
        since=None,
        until=None,
        usage_sink=None,
    ):
        commit = GitCommit(
            repo_id=repo_id,
            hash="gitlab123",
            message="msg",
            author_name="bob",
            author_email=None,
            author_when=datetime.now(timezone.utc),
            committer_name="bob",
            committer_email=None,
            committer_when=datetime.now(timezone.utc),
            parents=1,
        )
        return ["raw"], [commit]

    def fake_fetch_commit_stats(
        connector,
        project_id,
        commit_hashes,
        repo_id,
        max_stats,
        gate=None,
        usage_sink=None,
    ):
        return [
            GitCommitStat(
                repo_id=repo_id,
                commit_hash="gitlab123",
                file_path="file.txt",
                additions=2,
                deletions=1,
                old_file_mode="unknown",
                new_file_mode="unknown",
            )
        ]

    class DummyProjects:
        def get(self, project_id):
            return object()

    class DummyGitlab:
        def __init__(self):
            self.projects = DummyProjects()

    class DummyRestClient:
        def get_merge_requests(self, **kwargs):
            return []

    class DummyConnector:
        def __init__(self, url: str, private_token: str):
            self.url = url
            self.private_token = private_token
            self.gitlab = DummyGitlab()
            self.rest_client = DummyRestClient()

        async def get_projects_with_stats_async(self, **kwargs):
            on_project_complete = kwargs.get("on_project_complete")
            if on_project_complete:
                on_project_complete(result)
            return [result]

        def _get_projects_for_processing(self, **kwargs):
            return [project]

        def close(self):
            return

    monkeypatch.setattr(processors.gitlab, "GitLabConnector", DummyConnector)
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_commits_sync", fake_fetch_commits
    )
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_commit_stats_sync", fake_fetch_commit_stats
    )

    store = DummyStore()

    await processors.gitlab.process_gitlab_projects_batch(
        store=store,
        token="test_token",
        gitlab_url="https://gitlab.com",
        group_name="group",
        pattern="group/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
    )

    assert any(c.hash == "gitlab123" for c in recorded_commits)
    expected_repo_id = get_repo_uuid_from_repo(project.full_name)
    assert all(c.repo_id == expected_repo_id for c in recorded_commits)
    assert "gitlab123" in {s.commit_hash for s in recorded_stats}


@pytest.mark.asyncio
async def test_process_gitlab_projects_batch_since_prepass_persists_aggregate_stats(
    monkeypatch,
):
    _enable_connector_stubs(monkeypatch)

    since = datetime(2026, 5, 13, tzinfo=timezone.utc)
    recorded_stats: list[GitCommitStat] = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    project = Mock()
    project.id = 457
    project.full_name = "group/windowed-stats"
    project.url = "https://example.com/group/windowed-stats"
    project.default_branch = "main"

    class DummyConnector:
        def __init__(self, url: str, private_token: str):
            self.url = url
            self.private_token = private_token

        def close(self):
            return

    class DummyCodeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def list_projects(self, **kwargs):
            return [project]

        async def get_commit_stats(self, project_id, commit_hash):
            assert project_id == project.id
            assert commit_hash == "gitlab-windowed"
            return SimpleNamespace(additions=7, deletions=2)

        def drain_usage_observations(self):
            return []

    def fake_fetch_commits(
        connector,
        project_id,
        max_commits,
        repo_id,
        since=None,
        until=None,
        usage_sink=None,
    ):
        assert project_id == project.id
        assert since is not None
        return ["gitlab-windowed"], []

    monkeypatch.setattr(processors.gitlab, "GitLabConnector", DummyConnector)
    monkeypatch.setattr(
        processors.gitlab,
        "_gitlab_code_client_from_connector",
        lambda connector: DummyCodeClient(),
    )
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_commits_sync", fake_fetch_commits
    )

    await processors.gitlab.process_gitlab_projects_batch(
        store=DummyStore(),
        token="test_token",
        gitlab_url="https://gitlab.com",
        group_name="group",
        pattern="group/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
        sync_git=True,
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=False,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
        backfill_missing=False,
        since=since,
    )

    aggregate_stats = [
        stat
        for stat in recorded_stats
        if stat.commit_hash == processors.gitlab.AGGREGATE_STATS_MARKER
    ]
    assert len(aggregate_stats) == 1
    assert aggregate_stats[0].additions == 7
    assert aggregate_stats[0].deletions == 2


@pytest.mark.asyncio
async def test_process_gitlab_projects_batch_commit_rate_limit_propagates(
    monkeypatch,
):
    """CHAOS-2814/CS13: an exhausted RateLimitException raised while fetching
    commits/commit-stats for one project in the batch must propagate out of
    ``process_gitlab_projects_batch`` (mirrors
    ``process_github_repos_batch``'s ``except (RateLimitException,
    RateLimitExceededException): raise`` before the broad
    ``except Exception`` in ``store_result``), not be swallowed as a
    per-project warning."""
    _enable_connector_stubs(monkeypatch)

    recorded_stats: list[Any] = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    project = Mock()
    project.id = 501
    project.full_name = "group/rate-limited"
    project.url = "https://example.com/group/rate-limited"
    project.default_branch = "main"

    result = BatchResult(repository=project, stats=None, success=True)
    _stub_gitlab_project_discovery(monkeypatch, [project])

    def fake_fetch_commits(*args, **kwargs):
        raise RateLimitException("limited", retry_after_seconds=42.0)

    class DummyConnector:
        def __init__(self, url: str, private_token: str):
            self.url = url
            self.private_token = private_token

        async def get_projects_with_stats_async(self, **kwargs):
            on_project_complete = kwargs.get("on_project_complete")
            if on_project_complete:
                on_project_complete(result)
            return [result]

        def close(self):
            return

    monkeypatch.setattr(processors.gitlab, "GitLabConnector", DummyConnector)
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_commits_sync", fake_fetch_commits
    )

    with pytest.raises(RateLimitException) as exc_info:
        await processors.gitlab.process_gitlab_projects_batch(
            store=DummyStore(),
            token="test_token",
            gitlab_url="https://gitlab.com",
            group_name="group",
            pattern="group/*",
            batch_size=1,
            max_concurrent=1,
            rate_limit_delay=0,
            use_async=True,
            sync_prs=False,
            sync_cicd=False,
            sync_deployments=False,
            sync_incidents=False,
            sync_security=False,
            sync_tests=False,
            backfill_missing=False,
        )

    assert exc_info.value.retry_after_seconds == 42.0
    assert recorded_stats == []


@pytest.mark.asyncio
async def test_process_gitlab_projects_batch_multi_project_rate_limit_does_not_hang(
    monkeypatch,
):
    """The consumer-death FIRST_COMPLETED guard (mirrors
    ``process_github_repos_batch``) must stop ``results_queue.join()`` from
    hanging forever when the consumer task dies mid-batch with un-task_done()'d
    items still queued behind the failing project."""
    _enable_connector_stubs(monkeypatch)

    recorded_stats: list[Any] = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_git_commit_data(self, commit_data):
            return

        async def insert_git_commit_stats(self, commit_stats):
            recorded_stats.extend(commit_stats)

        async def insert_git_pull_requests(self, pr_data):
            return

    def make_result(index: int):
        project = Mock()
        project.id = index
        project.full_name = f"group/rate-limited-{index}"
        project.url = f"https://example.com/group/rate-limited-{index}"
        project.default_branch = "main"
        return BatchResult(repository=project, stats=None, success=True)

    results = [make_result(index) for index in range(3)]
    _stub_gitlab_project_discovery(
        monkeypatch, [result.repository for result in results]
    )

    def fake_fetch_commits(*args, **kwargs):
        raise RateLimitException("limited", retry_after_seconds=42.0)

    class DummyConnector:
        def __init__(self, url: str, private_token: str):
            self.url = url
            self.private_token = private_token

        async def get_projects_with_stats_async(self, **kwargs):
            on_project_complete = kwargs.get("on_project_complete")
            if on_project_complete:
                for result in results:
                    on_project_complete(result)
            return results

        def close(self):
            return

    monkeypatch.setattr(processors.gitlab, "GitLabConnector", DummyConnector)
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_commits_sync", fake_fetch_commits
    )

    with pytest.raises(RateLimitException) as exc_info:
        await asyncio.wait_for(
            processors.gitlab.process_gitlab_projects_batch(
                store=DummyStore(),
                token="test_token",
                gitlab_url="https://gitlab.com",
                group_name="group",
                pattern="group/*",
                batch_size=1,
                max_concurrent=1,
                rate_limit_delay=0,
                use_async=True,
                sync_prs=False,
                sync_cicd=False,
                sync_deployments=False,
                sync_incidents=False,
                sync_security=False,
                sync_tests=False,
                backfill_missing=False,
            ),
            timeout=2,
        )

    assert exc_info.value.retry_after_seconds == 42.0
    assert recorded_stats == []


@pytest.mark.asyncio
async def test_process_gitlab_projects_batch_threads_code_usage_sink(monkeypatch):
    _enable_connector_stubs(monkeypatch)
    seen_usage_sink_ids: list[int] = []
    seen_usage_sink_values: list[dict[str, object]] = []

    class DummyStore:
        async def insert_repo(self, repo):
            return

        async def insert_ci_pipeline_runs(self, pipeline_runs):
            return

        async def insert_deployments(self, deployments):
            return

    project = Mock()
    project.id = 456
    project.full_name = "group/proj-usage"
    project.url = "https://example.com/group/proj-usage"
    project.default_branch = "main"
    result = BatchResult(repository=project, stats=None, success=True)
    _stub_gitlab_project_discovery(monkeypatch, [project])

    class DummyConnector:
        def __init__(self, url: str, private_token: str):
            self.url = url
            self.private_token = private_token

        async def get_projects_with_stats_async(self, **kwargs):
            on_project_complete = kwargs.get("on_project_complete")
            if on_project_complete:
                on_project_complete(result)
            return [result]

        def _get_projects_for_processing(self, **kwargs):
            return [project]

        def close(self):
            return

    def fake_fetch_pipelines(
        connector, project_id, repo_id, max_pipelines, since, usage_sink=None
    ):
        assert usage_sink is not None
        seen_usage_sink_ids.append(id(usage_sink))
        usage_sink.append({"route_family": "pipelines"})
        seen_usage_sink_values.extend(usage_sink)
        return []

    def fake_fetch_deployments(
        connector, project_id, repo_id, max_deployments, since, usage_sink=None
    ):
        assert usage_sink is not None
        seen_usage_sink_ids.append(id(usage_sink))
        usage_sink.append({"route_family": "deployments"})
        seen_usage_sink_values.extend(usage_sink)
        return []

    monkeypatch.setattr(processors.gitlab, "GitLabConnector", DummyConnector)
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_pipelines_sync", fake_fetch_pipelines
    )
    monkeypatch.setattr(
        processors.gitlab, "_fetch_gitlab_deployments_sync", fake_fetch_deployments
    )

    await processors.gitlab.process_gitlab_projects_batch(
        store=DummyStore(),
        token="unit-token",
        gitlab_url="https://gitlab.com",
        group_name="group",
        pattern="group/*",
        batch_size=1,
        max_concurrent=1,
        rate_limit_delay=0,
        use_async=True,
        sync_git=False,
        sync_prs=False,
        sync_cicd=True,
        sync_deployments=True,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
        backfill_missing=False,
    )

    assert len(set(seen_usage_sink_ids)) == 1
    assert {item["route_family"] for item in seen_usage_sink_values} == {
        "pipelines",
        "deployments",
    }


class TestPatternMatching:
    """Test repository pattern matching functionality."""

    def test_exact_match(self):
        """Test exact repository name matching."""
        assert match_repo_pattern(
            "chrisgeo/mergestat-syncs", "chrisgeo/mergestat-syncs"
        )

    def test_wildcard_suffix(self):
        """Test pattern with wildcard suffix."""
        assert match_repo_pattern("chrisgeo/mergestat-syncs", "chrisgeo/merge*")
        assert match_repo_pattern("chrisgeo/mergestat", "chrisgeo/merge*")
        assert not match_repo_pattern("chrisgeo/other-repo", "chrisgeo/merge*")

    def test_wildcard_prefix(self):
        """Test pattern with wildcard prefix."""
        assert match_repo_pattern("chrisgeo/api-service", "*-service")
        assert match_repo_pattern("org/web-service", "*-service")

    def test_wildcard_owner(self):
        """Test pattern with wildcard owner."""
        assert match_repo_pattern("chrisgeo/sync-tool", "*/sync-tool")
        assert match_repo_pattern("otherorg/sync-tool", "*/sync-tool")

    def test_wildcard_repo(self):
        """Test pattern with wildcard repo."""
        assert match_repo_pattern("chrisgeo/anything", "chrisgeo/*")
        assert match_repo_pattern("chrisgeo/another", "chrisgeo/*")

    def test_case_insensitive(self):
        """Test case insensitive matching."""
        assert match_repo_pattern("ChrisGeo/MergeStat-Syncs", "chrisgeo/mergestat*")
        assert match_repo_pattern("CHRISGEO/REPO", "chrisgeo/*")

    def test_question_mark_wildcard(self):
        """Test question mark wildcard matching single character."""
        assert match_repo_pattern("chrisgeo/api-v1", "chrisgeo/api-v?")
        assert match_repo_pattern("chrisgeo/api-v2", "chrisgeo/api-v?")
        assert not match_repo_pattern("chrisgeo/api-v10", "chrisgeo/api-v?")

    def test_double_wildcard(self):
        """Test double wildcard matching."""
        assert match_repo_pattern("org/sub-api-service", "*api*")
        assert match_repo_pattern("chrisgeo/my-api", "*api*")

    def test_no_match(self):
        """Test non-matching patterns."""
        assert not match_repo_pattern("chrisgeo/repo", "other/*")
        assert not match_repo_pattern("org/api", "org/web*")


class TestBatchResult:
    """Test BatchResult dataclass."""

    def test_successful_result(self):
        """Test creating a successful batch result."""
        repo = Mock()
        repo.full_name = "org/repo"
        stats = Mock()

        result = BatchResult(repository=repo, stats=stats, success=True)

        assert result.repository == repo
        assert result.stats == stats
        assert result.success is True
        assert result.error is None

    def test_failed_result(self):
        """Test creating a failed batch result."""
        repo = Mock()
        repo.full_name = "org/repo"

        result = BatchResult(
            repository=repo,
            error="API error",
            success=False,
        )

        assert result.repository == repo
        assert result.stats is None
        assert result.success is False
        assert result.error == "API error"


class TestGitLabPatternMatching:
    """Test GitLab project pattern matching functionality."""

    def test_exact_match(self):
        """Test exact project name matching."""
        from dev_health_ops.connectors.utils import match_project_pattern

        assert match_project_pattern("group/project", "group/project")

    def test_wildcard_suffix(self):
        """Test pattern with wildcard suffix."""
        from dev_health_ops.connectors.utils import match_project_pattern

        assert match_project_pattern("group/api-service", "group/api-*")
        assert match_project_pattern("group/api-v2", "group/api-*")
        assert not match_project_pattern("group/web-service", "group/api-*")

    def test_wildcard_prefix(self):
        """Test pattern with wildcard prefix."""
        from dev_health_ops.connectors.utils import match_project_pattern

        assert match_project_pattern("mygroup/api-service", "*-service")
        assert match_project_pattern("other/web-service", "*-service")

    def test_wildcard_group(self):
        """Test pattern with wildcard group."""
        from dev_health_ops.connectors.utils import match_project_pattern

        assert match_project_pattern("group1/sync-tool", "*/sync-tool")
        assert match_project_pattern("group2/sync-tool", "*/sync-tool")

    def test_wildcard_project(self):
        """Test pattern with wildcard project."""
        from dev_health_ops.connectors.utils import match_project_pattern

        assert match_project_pattern("mygroup/anything", "mygroup/*")
        assert match_project_pattern("mygroup/another", "mygroup/*")

    def test_case_insensitive(self):
        """Test case insensitive matching."""
        from dev_health_ops.connectors.utils import match_project_pattern

        assert match_project_pattern("MyGroup/MyProject", "mygroup/myproject*")
        assert match_project_pattern("MYGROUP/PROJECT", "mygroup/*")


class TestGitLabBatchResult:
    """Test legacy GitLab result compatibility."""

    def test_successful_result(self):
        """Test creating a successful batch result."""
        project = Mock()
        project.full_name = "group/project"
        stats = Mock()

        result = BatchResult(repository=project, stats=stats, success=True)

        assert result.repository == project
        assert result.stats == stats
        assert result.success is True
        assert result.error is None

    def test_failed_result(self):
        """Test creating a failed batch result."""
        project = Mock()
        project.full_name = "group/project"

        result = BatchResult(
            repository=project,
            error="API error",
            success=False,
        )

        assert result.repository == project
        assert result.stats is None
        assert result.success is False
        assert result.error == "API error"
