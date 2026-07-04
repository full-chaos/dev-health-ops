from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from dev_health_ops.processors import github, gitlab


def _counting(counter: dict[str, Any], key: str, value: Any):
    """Return a fake provider call that bumps ``counter[key]`` and returns value."""

    def _inner(*args: Any, **kwargs: Any) -> Any:
        counter[key] = counter.get(key, 0) + 1
        return value

    return _inner


def _async_counting(counter: dict[str, Any], key: str, value: Any):
    async def _inner(*args: Any, **kwargs: Any) -> Any:
        counter[key] = counter.get(key, 0) + 1
        return value

    return _inner


async def _fake_fetch_github_repo_info(connector, owner, repo_name, usage_sink=None):
    """Mirror ``_fetch_github_repo_info_async``'s field mapping so tests keep
    exercising ``_FakeGitHubConnector.github.get_repo`` without a real
    ``GitHubCodeClient`` (these fixtures pin PyGithub-shaped attribute names).
    """
    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")
    return github.Repository(
        id=gh_repo.id,
        name=gh_repo.name,
        full_name=gh_repo.full_name,
        default_branch=gh_repo.default_branch,
        description=gh_repo.description,
        url=gh_repo.html_url,
        created_at=gh_repo.created_at,
        updated_at=gh_repo.updated_at,
        language=gh_repo.language,
        stars=gh_repo.stargazers_count,
        forks=gh_repo.forks_count,
    )


class _SimpleRepository:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FakeSink:
    def __init__(self, store):
        store.sink = self
        self.commits = []
        self.stats = []
        self.repos = []

    async def insert_repo(self, repo):
        repo.id = "repo-1"
        self.repos.append(repo)

    async def insert_git_commit_data(self, commits):
        self.commits.extend(commits)

    async def insert_git_commit_stats(self, stats):
        self.stats.extend(stats)


class _FakeStore:
    org_id = "org-1"

    def __init__(self):
        self.sink: Any = None


class _FakeGitHubConnector:
    def __init__(self, *args, **kwargs):
        self.github = SimpleNamespace(get_repo=lambda full_name: _fake_github_repo())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeGitLabConnector:
    def __init__(self, *args, **kwargs):
        self.gitlab = SimpleNamespace(
            projects=SimpleNamespace(get=lambda project_id: _fake_gitlab_project())
        )

    def close(self):
        return None


def _fake_github_repo():
    return SimpleNamespace(
        id=1,
        name="repo",
        full_name="org/repo",
        default_branch="main",
        description=None,
        html_url="https://example.test/org/repo",
        created_at=None,
        updated_at=None,
        language="Python",
        stargazers_count=0,
        forks_count=0,
    )


def _fake_gitlab_project():
    return SimpleNamespace(
        id=1,
        name="repo",
        path_with_namespace="org/repo",
        default_branch="main",
        web_url="https://example.test/org/repo",
    )


def _disable_non_git_flags() -> dict[str, Any]:
    return dict(
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=False,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
    )


def test_github_commits_run_does_not_fetch_stats_files_or_blame(monkeypatch):
    calls: dict[str, Any] = {"commits": 0, "stats": 0, "backfill": []}

    monkeypatch.setattr(github, "CONNECTORS_AVAILABLE", True)
    monkeypatch.setattr(github, "GitHubConnector", _FakeGitHubConnector)
    monkeypatch.setattr(github, "Repository", _SimpleRepository)
    monkeypatch.setattr(github, "IngestionSink", _FakeSink)
    monkeypatch.setattr(
        github, "_fetch_github_repo_info_async", _fake_fetch_github_repo_info
    )
    monkeypatch.setattr(
        github,
        "_fetch_github_commits_async",
        _async_counting(calls, "commits", (["raw-sha"], ["commit-row"], False)),
    )
    monkeypatch.setattr(
        github,
        "_fetch_github_commit_stats_async",
        _async_counting(calls, "stats", ["stat-row"]),
    )

    async def fake_backfill(**kwargs):
        calls["backfill"].append(kwargs)

    monkeypatch.setattr(github, "_backfill_github_missing_data", fake_backfill)
    store = _FakeStore()

    asyncio.run(
        github.process_github_repo(
            store,
            "org",
            "repo",
            "token",
            sync_git=False,
            sync_commits=True,
            sync_commit_stats=False,
            sync_files=False,
            sync_blame=False,
            **_disable_non_git_flags(),
        )
    )

    assert calls == {"commits": 1, "stats": 0, "backfill": []}
    assert store.sink.commits == ["commit-row"]
    assert store.sink.stats == []


def test_github_granular_stats_files_blame_and_legacy_bundle(monkeypatch):
    calls: dict[str, Any] = {"commits": 0, "stats": 0, "backfill": []}

    monkeypatch.setattr(github, "CONNECTORS_AVAILABLE", True)
    monkeypatch.setattr(github, "GitHubConnector", _FakeGitHubConnector)
    monkeypatch.setattr(github, "Repository", _SimpleRepository)
    monkeypatch.setattr(github, "IngestionSink", _FakeSink)
    monkeypatch.setattr(
        github, "_fetch_github_repo_info_async", _fake_fetch_github_repo_info
    )
    monkeypatch.setattr(
        github,
        "_fetch_github_commits_async",
        _async_counting(calls, "commits", (["raw-sha"], ["commit-row"], False)),
    )
    monkeypatch.setattr(
        github,
        "_fetch_github_commit_stats_async",
        _async_counting(calls, "stats", ["stat-row"]),
    )

    async def fake_backfill(**kwargs):
        calls["backfill"].append(
            (
                kwargs["include_files"],
                kwargs["include_blame"],
                kwargs["include_commit_stats"],
            )
        )

    monkeypatch.setattr(github, "_backfill_github_missing_data", fake_backfill)

    stats_store = _FakeStore()
    asyncio.run(
        github.process_github_repo(
            stats_store,
            "org",
            "repo",
            "token",
            sync_git=False,
            sync_commits=False,
            sync_commit_stats=True,
            sync_files=False,
            sync_blame=False,
            **_disable_non_git_flags(),
        )
    )
    assert stats_store.sink.commits == []
    assert stats_store.sink.stats == ["stat-row"]

    asyncio.run(
        github.process_github_repo(
            _FakeStore(),
            "org",
            "repo",
            "token",
            sync_git=False,
            sync_commits=False,
            sync_commit_stats=False,
            sync_files=True,
            sync_blame=False,
            **_disable_non_git_flags(),
        )
    )
    asyncio.run(
        github.process_github_repo(
            _FakeStore(),
            "org",
            "repo",
            "token",
            sync_git=False,
            sync_commits=False,
            sync_commit_stats=False,
            sync_files=False,
            sync_blame=True,
            **_disable_non_git_flags(),
        )
    )
    legacy_store = _FakeStore()
    asyncio.run(
        github.process_github_repo(
            legacy_store,
            "org",
            "repo",
            "token",
            sync_git=True,
            **_disable_non_git_flags(),
        )
    )

    assert calls["backfill"] == [
        (True, False, False),
        (False, True, False),
        (True, True, False),
    ]
    assert legacy_store.sink.commits == ["commit-row"]
    assert legacy_store.sink.stats == ["stat-row"]


def test_gitlab_commits_run_does_not_fetch_stats_files_or_blame(monkeypatch):
    calls: dict[str, Any] = {"commits": 0, "stats": 0, "backfill": []}

    monkeypatch.setattr(gitlab, "CONNECTORS_AVAILABLE", True)
    monkeypatch.setattr(gitlab, "GitLabConnector", _FakeGitLabConnector)
    monkeypatch.setattr(gitlab, "IngestionSink", _FakeSink)
    monkeypatch.setattr(
        gitlab,
        "_fetch_gitlab_commits_sync",
        _counting(calls, "commits", (["sha"], ["commit-row"])),
    )
    monkeypatch.setattr(
        gitlab,
        "_fetch_gitlab_commit_stats_sync",
        _counting(calls, "stats", ["stat-row"]),
    )

    async def fake_backfill(**kwargs):
        calls["backfill"].append(kwargs)

    monkeypatch.setattr(gitlab, "_backfill_gitlab_missing_data", fake_backfill)
    store = _FakeStore()

    asyncio.run(
        gitlab.process_gitlab_project(
            store,
            1,
            "token",
            "https://gitlab.example",
            sync_git=False,
            sync_commits=True,
            sync_commit_stats=False,
            sync_files=False,
            sync_blame=False,
            **_disable_non_git_flags(),
        )
    )

    assert calls == {"commits": 1, "stats": 0, "backfill": []}
    assert store.sink.commits == ["commit-row"]
    assert store.sink.stats == []


def test_gitlab_granular_stats_files_blame_and_legacy_bundle(monkeypatch):
    calls: dict[str, Any] = {"commits": 0, "stats": 0, "backfill": []}

    monkeypatch.setattr(gitlab, "CONNECTORS_AVAILABLE", True)
    monkeypatch.setattr(gitlab, "GitLabConnector", _FakeGitLabConnector)
    monkeypatch.setattr(gitlab, "IngestionSink", _FakeSink)
    monkeypatch.setattr(
        gitlab,
        "_fetch_gitlab_commits_sync",
        _counting(calls, "commits", (["sha"], ["commit-row"])),
    )
    monkeypatch.setattr(
        gitlab,
        "_fetch_gitlab_commit_stats_sync",
        _counting(calls, "stats", ["stat-row"]),
    )

    async def fake_backfill(**kwargs):
        calls["backfill"].append(
            (
                kwargs["include_files"],
                kwargs["include_blame"],
                kwargs["include_commit_stats"],
            )
        )

    monkeypatch.setattr(gitlab, "_backfill_gitlab_missing_data", fake_backfill)

    stats_store = _FakeStore()
    asyncio.run(
        gitlab.process_gitlab_project(
            stats_store,
            1,
            "token",
            "https://gitlab.example",
            sync_git=False,
            sync_commits=False,
            sync_commit_stats=True,
            sync_files=False,
            sync_blame=False,
            **_disable_non_git_flags(),
        )
    )
    assert stats_store.sink.commits == []
    assert stats_store.sink.stats == ["stat-row"]

    asyncio.run(
        gitlab.process_gitlab_project(
            _FakeStore(),
            1,
            "token",
            "https://gitlab.example",
            sync_git=False,
            sync_commits=False,
            sync_commit_stats=False,
            sync_files=True,
            sync_blame=False,
            **_disable_non_git_flags(),
        )
    )
    asyncio.run(
        gitlab.process_gitlab_project(
            _FakeStore(),
            1,
            "token",
            "https://gitlab.example",
            sync_git=False,
            sync_commits=False,
            sync_commit_stats=False,
            sync_files=False,
            sync_blame=True,
            **_disable_non_git_flags(),
        )
    )
    legacy_store = _FakeStore()
    asyncio.run(
        gitlab.process_gitlab_project(
            legacy_store,
            1,
            "token",
            "https://gitlab.example",
            sync_git=True,
            **_disable_non_git_flags(),
        )
    )

    assert calls["backfill"] == [
        (True, False, False),
        (False, True, False),
        (True, True, False),
    ]
    assert legacy_store.sink.commits == ["commit-row"]
    assert legacy_store.sink.stats == ["stat-row"]
