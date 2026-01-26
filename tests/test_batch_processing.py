"""
Tests for batch repository processing features.
"""

import asyncio
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from dev_health_ops.connectors import (
    GitHubConnector,
    GitLabConnector,
    BatchResult,
    match_repo_pattern,
)
from dev_health_ops.models.git import GitCommit, GitCommitStat, get_repo_uuid_from_repo
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


@pytest.mark.asyncio
async def test_github_async_batch_callback_fires_as_completed(monkeypatch):
    """Fast repos should invoke callback before slow repos in same batch."""
    from dev_health_ops.connectors.models import Repository

    with (
        patch("dev_health_ops.connectors.github.Github"),
        patch("dev_health_ops.connectors.github.GitHubGraphQLClient"),
    ):
        connector = GitHubConnector(token="test_token")

    slow = Repository(
        id=1,
        name="slow",
        full_name="org/slow",
        default_branch="main",
        url="https://example.com/org/slow",
    )
    fast = Repository(
        id=2,
        name="fast",
        full_name="org/fast",
        default_branch="main",
        url="https://example.com/org/fast",
    )

    # Ensure input ordering doesn't accidentally match completion ordering.
    monkeypatch.setattr(
        connector,
        "_get_repositories_for_processing",
        lambda **kwargs: [slow, fast],
    )

    def fake_process(repo, max_commits):
        if repo.name == "slow":
            time.sleep(0.15)
        else:
            time.sleep(0.01)
        return BatchResult(repository=repo, stats=None, success=True)

    monkeypatch.setattr(connector, "_process_single_repo_stats", fake_process)

    callback_order = []

    await connector.get_repos_with_stats_async(
        org_name="org",
        batch_size=2,
        max_concurrent=2,
        rate_limit_delay=0,
        on_repo_complete=lambda r: callback_order.append(r.repository.name),
    )

    assert callback_order[0] == "fast"
    assert callback_order[1] == "slow"


@pytest.mark.asyncio
async def test_gitlab_async_batch_callback_fires_as_completed(monkeypatch):
    """Fast projects should invoke callback before slow projects in same batch."""
    from dev_health_ops.connectors.models import Repository

    with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab"):
        connector = GitLabConnector(url="https://gitlab.com", private_token="test")

    slow = Repository(
        id=1,
        name="slow",
        full_name="group/slow",
        default_branch="main",
        url="https://example.com/group/slow",
    )
    fast = Repository(
        id=2,
        name="fast",
        full_name="group/fast",
        default_branch="main",
        url="https://example.com/group/fast",
    )

    monkeypatch.setattr(
        connector,
        "_get_repositories_for_processing",
        lambda **kwargs: [slow, fast],
    )

    def fake_process(project, max_commits):
        if project.name == "slow":
            time.sleep(0.15)
        else:
            time.sleep(0.01)
        return BatchResult(repository=project, stats=None, success=True)

    monkeypatch.setattr(connector, "_process_single_repo_stats", fake_process)

    callback_order = []

    await connector.get_projects_with_stats_async(
        group_name="group",
        batch_size=2,
        max_concurrent=2,
        rate_limit_delay=0,
        on_project_complete=lambda r: callback_order.append(r.repository.name),
    )

    assert callback_order[0] == "fast"
    assert callback_order[1] == "slow"


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
async def test_process_github_repos_batch_upserts_during_async_processing(monkeypatch):
    """Ensure async batch mode upserts as repos complete."""
    _enable_connector_stubs(monkeypatch)

    monkeypatch.setattr(
        processors.github,
        "_fetch_github_commits_sync",
        lambda *args, **kwargs: ([], []),
    )
    monkeypatch.setattr(
        processors.github, "_fetch_github_commit_stats_sync", lambda *args, **kwargs: []
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

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)

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

    def fake_fetch_commit_stats(raw_commits, repo_id, max_stats, since=None):
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

    monkeypatch.setattr(processors.github, "GitHubConnector", DummyConnector)
    monkeypatch.setattr(
        processors.github, "_fetch_github_commits_sync", fake_fetch_commits
    )
    monkeypatch.setattr(
        processors.github, "_fetch_github_commit_stats_sync", fake_fetch_commit_stats
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

    def fake_fetch_commits(gl_project, max_commits, repo_id, since=None):
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

    def fake_fetch_commit_stats(gl_project, commit_hashes, repo_id, max_stats):
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


class TestGitHubConnectorBatchProcessing:
    """Test GitHub connector batch processing features."""

    @pytest.fixture
    def mock_github_client(self):
        """Create a mock GitHub client."""
        with patch("dev_health_ops.connectors.github.Github") as mock_github:
            yield mock_github

    @pytest.fixture
    def mock_graphql_client(self):
        """Create a mock GraphQL client."""
        with patch("dev_health_ops.connectors.github.GitHubGraphQLClient") as mock_graphql:
            yield mock_graphql

    def _create_mock_repo(self, name: str, full_name: str):
        """Create a mock repository."""
        mock_repo = Mock()
        mock_repo.id = hash(full_name)
        mock_repo.name = name
        mock_repo.full_name = full_name
        mock_repo.default_branch = "main"
        mock_repo.description = f"Test repository {name}"
        mock_repo.html_url = f"https://github.com/{full_name}"
        mock_repo.created_at = None
        mock_repo.updated_at = None
        mock_repo.language = "Python"
        mock_repo.stargazers_count = 10
        mock_repo.forks_count = 5
        return mock_repo

    def test_list_repositories_with_pattern(
        self, mock_github_client, mock_graphql_client
    ):
        """Test listing repositories with pattern matching."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("mergestat-syncs", "chrisgeo/mergestat-syncs"),
            self._create_mock_repo("mergestat-lite", "chrisgeo/mergestat-lite"),
            self._create_mock_repo("other-repo", "chrisgeo/other-repo"),
            self._create_mock_repo("api-service", "chrisgeo/api-service"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Test pattern matching using list_repositories with pattern parameter
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories(
            user_name="chrisgeo",
            pattern="chrisgeo/merge*",
        )

        # Should only return repos matching the pattern
        assert len(repos) == 2
        assert all("merge" in repo.full_name.lower() for repo in repos)

    def test_list_repositories_with_pattern_max_repos(
        self, mock_github_client, mock_graphql_client
    ):
        """Test list_repositories with pattern respects max_repos."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo(f"mergestat-{i}", f"chrisgeo/mergestat-{i}")
            for i in range(10)
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Test with max_repos limit
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories(
            user_name="chrisgeo",
            pattern="chrisgeo/merge*",
            max_repos=3,
        )

        assert len(repos) == 3

    def test_get_repos_with_stats_sync(self, mock_github_client, mock_graphql_client):
        """Test synchronous batch processing of repositories with stats."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "chrisgeo/repo1"),
            self._create_mock_repo("repo2", "chrisgeo/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo (used by get_repo_stats)
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test batch processing
        connector = GitHubConnector(token="test_token")
        results = connector.get_repos_with_stats(
            user_name="chrisgeo",
            batch_size=2,
            max_concurrent=2,
            rate_limit_delay=0.1,  # Small delay for testing
        )

        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)

    def test_get_repos_with_stats_with_pattern(
        self, mock_github_client, mock_graphql_client
    ):
        """Test batch processing with pattern filtering."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("api-v1", "org/api-v1"),
            self._create_mock_repo("api-v2", "org/api-v2"),
            self._create_mock_repo("web-app", "org/web-app"),
        ]

        mock_org = Mock()
        mock_org.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_organization.return_value = mock_org

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test with pattern
        connector = GitHubConnector(token="test_token")
        results = connector.get_repos_with_stats(
            org_name="org",
            pattern="org/api-*",
            batch_size=5,
            rate_limit_delay=0.1,
        )

        # Should only process repos matching pattern
        assert len(results) == 2
        assert all("api-" in r.repository.full_name for r in results)

    def test_get_repos_with_stats_callback(
        self, mock_github_client, mock_graphql_client
    ):
        """Test callback is called for each processed repository."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "user/repo1"),
            self._create_mock_repo("repo2", "user/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test callback
        callback = Mock()
        connector = GitHubConnector(token="test_token")
        _ = connector.get_repos_with_stats(
            user_name="user",
            on_repo_complete=callback,
            rate_limit_delay=0.1,
        )

        # Callback should be called for each repo
        assert callback.call_count == 2


class TestGitHubConnectorAsyncBatchProcessing:
    """Test GitHub connector async batch processing features."""

    @pytest.fixture
    def mock_github_client(self):
        """Create a mock GitHub client."""
        with patch("dev_health_ops.connectors.github.Github") as mock_github:
            yield mock_github

    @pytest.fixture
    def mock_graphql_client(self):
        """Create a mock GraphQL client."""
        with patch("dev_health_ops.connectors.github.GitHubGraphQLClient") as mock_graphql:
            yield mock_graphql

    def _create_mock_repo(self, name: str, full_name: str):
        """Create a mock repository."""
        mock_repo = Mock()
        mock_repo.id = hash(full_name)
        mock_repo.name = name
        mock_repo.full_name = full_name
        mock_repo.default_branch = "main"
        mock_repo.description = f"Test repository {name}"
        mock_repo.html_url = f"https://github.com/{full_name}"
        mock_repo.created_at = None
        mock_repo.updated_at = None
        mock_repo.language = "Python"
        mock_repo.stargazers_count = 10
        mock_repo.forks_count = 5
        return mock_repo

    @pytest.mark.asyncio
    async def test_get_repos_with_stats_async(
        self, mock_github_client, mock_graphql_client
    ):
        """Test async batch processing of repositories with stats."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "chrisgeo/repo1"),
            self._create_mock_repo("repo2", "chrisgeo/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test async batch processing
        connector = GitHubConnector(token="test_token")
        results = await connector.get_repos_with_stats_async(
            user_name="chrisgeo",
            batch_size=2,
            max_concurrent=2,
            rate_limit_delay=0.1,
        )

        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)

    @pytest.mark.asyncio
    async def test_get_repos_with_stats_async_with_pattern(
        self, mock_github_client, mock_graphql_client
    ):
        """Test async batch processing with pattern filtering."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("api-v1", "org/api-v1"),
            self._create_mock_repo("api-v2", "org/api-v2"),
            self._create_mock_repo("web-app", "org/web-app"),
        ]

        mock_org = Mock()
        mock_org.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_organization.return_value = mock_org

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test with pattern
        connector = GitHubConnector(token="test_token")
        results = await connector.get_repos_with_stats_async(
            org_name="org",
            pattern="org/api-*",
            batch_size=5,
            rate_limit_delay=0.1,
        )

        # Should only process repos matching pattern
        assert len(results) == 2
        assert all("api-" in r.repository.full_name for r in results)

    @pytest.mark.asyncio
    async def test_get_repos_with_stats_async_callback(
        self, mock_github_client, mock_graphql_client
    ):
        """Test async callback is called for each processed repository."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "user/repo1"),
            self._create_mock_repo("repo2", "user/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test callback
        callback = Mock()
        connector = GitHubConnector(token="test_token")
        _ = await connector.get_repos_with_stats_async(
            user_name="user",
            on_repo_complete=callback,
            rate_limit_delay=0.1,
        )

        # Callback should be called for each repo
        assert callback.call_count == 2


class TestBatchProcessingErrorHandling:
    """Test error handling in batch repository processing."""

    @pytest.fixture
    def mock_github_client(self):
        """Create a mock GitHub client."""
        with patch("dev_health_ops.connectors.github.Github") as mock_github:
            yield mock_github

    @pytest.fixture
    def mock_graphql_client(self):
        """Create a mock GraphQL client."""
        with patch("dev_health_ops.connectors.github.GitHubGraphQLClient") as mock_graphql:
            yield mock_graphql

    def _create_mock_repo(self, name: str, full_name: str):
        """Create a mock repository."""
        mock_repo = Mock()
        mock_repo.id = hash(full_name)
        mock_repo.name = name
        mock_repo.full_name = full_name
        mock_repo.default_branch = "main"
        mock_repo.description = f"Test repository {name}"
        mock_repo.html_url = f"https://github.com/{full_name}"
        mock_repo.created_at = None
        mock_repo.updated_at = None
        mock_repo.language = "Python"
        mock_repo.stargazers_count = 10
        mock_repo.forks_count = 5
        return mock_repo

    def test_batch_processing_handles_api_error(
        self, mock_github_client, mock_graphql_client
    ):
        """Test batch processing continues when get_repo_stats raises an exception."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "user/repo1"),
            self._create_mock_repo("repo2", "user/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo to raise an exception
        mock_github_instance.get_repo.side_effect = Exception("API rate limit exceeded")

        # Test batch processing
        connector = GitHubConnector(token="test_token")
        results = connector.get_repos_with_stats(
            user_name="user",
            batch_size=2,
            rate_limit_delay=0.1,
        )

        # All repos should be processed, but with errors
        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)
        assert all(r.success is False for r in results)
        assert all(r.error is not None for r in results)
        assert all("API rate limit exceeded" in str(r.error) for r in results)

    def test_batch_processing_handles_partial_failure(
        self, mock_github_client, mock_graphql_client
    ):
        """Test batch processing handles some repos failing while others succeed."""
        from datetime import datetime, timezone

        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "user/repo1"),
            self._create_mock_repo("repo2", "user/repo2"),
            self._create_mock_repo("repo3", "user/repo3"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo - first succeeds, second fails, third succeeds
        mock_gh_repo_success = Mock()
        mock_gh_repo_success.get_commits.return_value = []
        mock_gh_repo_success.created_at = datetime.now(timezone.utc)

        def get_repo_side_effect(repo_name):
            if "repo2" in repo_name:
                raise Exception("Repository not found")
            return mock_gh_repo_success

        mock_github_instance.get_repo.side_effect = get_repo_side_effect

        # Test batch processing
        connector = GitHubConnector(token="test_token")
        results = connector.get_repos_with_stats(
            user_name="user",
            batch_size=3,
            rate_limit_delay=0.1,
        )

        # All repos should be processed
        assert len(results) == 3

        # Count successes and failures
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]

        assert len(successes) == 2
        assert len(failures) == 1

        # Verify the failed repo has the correct error
        failed_result = failures[0]
        assert "Repository not found" in str(failed_result.error)
        assert failed_result.repository.full_name == "user/repo2"

    def test_batch_processing_invalid_repo_name(
        self, mock_github_client, mock_graphql_client
    ):
        """Test batch processing handles invalid repository names."""
        # Setup mock repos with invalid name
        mock_repo_invalid = Mock()
        mock_repo_invalid.id = 1
        mock_repo_invalid.name = "invalid"
        mock_repo_invalid.full_name = (
            "invalid_no_slash"  # Invalid - no owner/repo format
        )
        mock_repo_invalid.default_branch = "main"
        mock_repo_invalid.description = "Invalid repo"
        mock_repo_invalid.html_url = "https://github.com/invalid"
        mock_repo_invalid.created_at = None
        mock_repo_invalid.updated_at = None
        mock_repo_invalid.language = "Python"
        mock_repo_invalid.stargazers_count = 0
        mock_repo_invalid.forks_count = 0

        mock_user = Mock()
        mock_user.get_repos.return_value = [mock_repo_invalid]

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Test batch processing
        connector = GitHubConnector(token="test_token")
        results = connector.get_repos_with_stats(
            user_name="user",
            rate_limit_delay=0.1,
        )

        # Should have one result with error
        assert len(results) == 1
        assert results[0].success is False
        assert "Invalid repository name" in str(results[0].error)

    @pytest.mark.asyncio
    async def test_async_batch_processing_handles_api_error(
        self, mock_github_client, mock_graphql_client
    ):
        """Test async batch processing continues when get_repo_stats raises an exception."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "user/repo1"),
            self._create_mock_repo("repo2", "user/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo to raise an exception
        mock_github_instance.get_repo.side_effect = Exception("API rate limit exceeded")

        # Test async batch processing
        connector = GitHubConnector(token="test_token")
        results = await connector.get_repos_with_stats_async(
            user_name="user",
            batch_size=2,
            rate_limit_delay=0.1,
        )

        # All repos should be processed, but with errors
        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)
        assert all(r.success is False for r in results)
        assert all(r.error is not None for r in results)


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


class TestGitLabConnectorBatchProcessing:
    """Test GitLab connector batch processing features."""

    @pytest.fixture
    def mock_gitlab_client(self):
        """Create a mock GitLab client."""
        with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
            mock_instance = mock_gitlab.return_value
            mock_instance.auth.return_value = None
            yield mock_gitlab

    @pytest.fixture
    def mock_rest_client(self):
        """Create a mock REST client."""
        with patch("dev_health_ops.connectors.gitlab.GitLabRESTClient") as mock_rest:
            yield mock_rest

    def _create_mock_project(self, name: str, full_name: str):
        """Create a mock project."""
        mock_project = Mock()
        mock_project.id = hash(full_name)
        mock_project.name = name
        mock_project.path_with_namespace = full_name
        mock_project.full_name = full_name
        mock_project.default_branch = "main"
        mock_project.description = f"Test project {name}"
        mock_project.web_url = f"https://gitlab.com/{full_name}"
        mock_project.created_at = "2024-01-01T00:00:00Z"
        mock_project.last_activity_at = "2024-01-01T00:00:00Z"
        mock_project.star_count = 10
        mock_project.forks_count = 5
        return mock_project

    def test_list_projects_with_pattern(self, mock_gitlab_client, mock_rest_client):
        """Test listing projects with pattern matching."""
        # Setup mock projects
        mock_projects = [
            self._create_mock_project("api-service", "group/api-service"),
            self._create_mock_project("api-v2", "group/api-v2"),
            self._create_mock_project("web-app", "group/web-app"),
            self._create_mock_project("cli-tool", "group/cli-tool"),
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Test pattern matching
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        projects = connector.list_projects(pattern="group/api-*")

        # Should only return projects matching the pattern
        assert len(projects) == 2
        assert all("api" in proj.full_name.lower() for proj in projects)

    def test_get_projects_with_stats_sync(self, mock_gitlab_client, mock_rest_client):
        """Test synchronous batch processing of projects with stats."""
        # Setup mock projects
        mock_projects = [
            self._create_mock_project("project1", "group/project1"),
            self._create_mock_project("project2", "group/project2"),
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Setup mock for get_repo_stats (via projects.get)
        mock_gl_project = Mock()
        mock_gl_project.commits.list.return_value = []
        mock_gl_project.created_at = "2024-01-01T00:00:00Z"
        mock_gitlab_instance.projects.get.return_value = mock_gl_project

        # Test batch processing
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        results = connector.get_projects_with_stats(
            batch_size=2,
            max_concurrent=2,
            rate_limit_delay=0.1,  # Small delay for testing
        )

        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)

    def test_get_projects_with_stats_with_pattern(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test batch processing with pattern filtering."""
        # Setup mock projects
        mock_projects = [
            self._create_mock_project("api-v1", "org/api-v1"),
            self._create_mock_project("api-v2", "org/api-v2"),
            self._create_mock_project("web-app", "org/web-app"),
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Setup mock group for pattern extraction (org/api-* -> group='org')
        mock_group = Mock()
        mock_group.projects.list.return_value = mock_projects
        mock_gitlab_instance.groups.get.return_value = mock_group

        # Setup mock for get_repo_stats
        mock_gl_project = Mock()
        mock_gl_project.commits.list.return_value = []
        mock_gl_project.created_at = "2024-01-01T00:00:00Z"
        mock_gitlab_instance.projects.get.return_value = mock_gl_project

        # Test with pattern
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        results = connector.get_projects_with_stats(
            pattern="org/api-*",
            batch_size=5,
            rate_limit_delay=0.1,
        )

        # Should only process projects matching pattern
        assert len(results) == 2
        assert all("api-" in r.repository.full_name for r in results)


@pytest.mark.asyncio
async def test_async_batch_processing_handles_api_error_placeholder():
    """Placeholder for async batch error handling tests."""
    pass


class TestGitLabConnectorAsyncBatchProcessing:
    """Test GitLab connector async batch processing features."""

    @pytest.fixture
    def mock_gitlab_client(self):
        """Create a mock GitLab client."""
        with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
            mock_instance = mock_gitlab.return_value
            mock_instance.auth.return_value = None
            yield mock_gitlab

    @pytest.fixture
    def mock_rest_client(self):
        """Create a mock REST client."""
        with patch("dev_health_ops.connectors.gitlab.GitLabRESTClient") as mock_rest:
            yield mock_rest

    def _create_mock_project(self, name: str, full_name: str):
        """Create a mock project."""
        mock_project = Mock()
        mock_project.id = hash(full_name)
        mock_project.name = name
        mock_project.path_with_namespace = full_name
        mock_project.full_name = full_name
        mock_project.default_branch = "main"
        mock_project.description = f"Test project {name}"
        mock_project.web_url = f"https://gitlab.com/{full_name}"
        mock_project.created_at = "2024-01-01T00:00:00Z"
        mock_project.last_activity_at = "2024-01-01T00:00:00Z"
        mock_project.star_count = 10
        mock_project.forks_count = 5
        return mock_project

    @pytest.mark.asyncio
    async def test_get_projects_with_stats_async(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test async batch processing of projects with stats."""
        # Setup mock projects
        mock_projects = [
            self._create_mock_project("project1", "group/project1"),
            self._create_mock_project("project2", "group/project2"),
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Setup mock for get_repo_stats
        mock_gl_project = Mock()
        mock_gl_project.commits.list.return_value = []
        mock_gl_project.created_at = "2024-01-01T00:00:00Z"
        mock_gitlab_instance.projects.get.return_value = mock_gl_project

        # Test async batch processing
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        results = await connector.get_projects_with_stats_async(
            batch_size=2,
            max_concurrent=2,
            rate_limit_delay=0.1,
        )

        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)

    @pytest.mark.asyncio
    async def test_get_projects_with_stats_async_with_pattern(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test async batch processing with pattern filtering."""
        # Setup mock projects
        mock_projects = [
            self._create_mock_project("api-v1", "org/api-v1"),
            self._create_mock_project("api-v2", "org/api-v2"),
            self._create_mock_project("web-app", "org/web-app"),
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Setup mock group for pattern extraction (org/api-* -> group='org')
        mock_group = Mock()
        mock_group.projects.list.return_value = mock_projects
        mock_gitlab_instance.groups.get.return_value = mock_group

        # Setup mock for get_repo_stats
        mock_gl_project = Mock()
        mock_gl_project.commits.list.return_value = []
        mock_gl_project.created_at = "2024-01-01T00:00:00Z"
        mock_gitlab_instance.projects.get.return_value = mock_gl_project

        # Test with pattern
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        results = await connector.get_projects_with_stats_async(
            pattern="org/api-*",
            batch_size=5,
            rate_limit_delay=0.1,
        )

        # Should only process projects matching pattern
        assert len(results) == 2
        assert all("api-" in r.repository.full_name for r in results)


class TestGitLabBatchProcessingErrorHandling:
    """Test error handling in GitLab batch project processing."""

    @pytest.fixture
    def mock_gitlab_client(self):
        """Create a mock GitLab client."""
        with patch("dev_health_ops.connectors.gitlab.gitlab.Gitlab") as mock_gitlab:
            mock_instance = mock_gitlab.return_value
            mock_instance.auth.return_value = None
            yield mock_gitlab

    @pytest.fixture
    def mock_rest_client(self):
        """Create a mock REST client."""
        with patch("dev_health_ops.connectors.gitlab.GitLabRESTClient") as mock_rest:
            yield mock_rest

    def _create_mock_project(self, name: str, full_name: str):
        """Create a mock project."""
        mock_project = Mock()
        mock_project.id = hash(full_name)
        mock_project.name = name
        mock_project.path_with_namespace = full_name
        mock_project.full_name = full_name
        mock_project.default_branch = "main"
        mock_project.description = f"Test project {name}"
        mock_project.web_url = f"https://gitlab.com/{full_name}"
        mock_project.created_at = "2024-01-01T00:00:00Z"
        mock_project.last_activity_at = "2024-01-01T00:00:00Z"
        mock_project.star_count = 10
        mock_project.forks_count = 5
        return mock_project

    def test_batch_processing_handles_api_error(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test batch processing continues when get_repo_stats raises an exception."""
        # Setup mock projects
        mock_projects = [
            self._create_mock_project("project1", "group/project1"),
            self._create_mock_project("project2", "group/project2"),
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Setup mock for projects.get to raise an exception
        mock_gitlab_instance.projects.get.side_effect = Exception(
            "API rate limit exceeded"
        )

        # Test batch processing
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        results = connector.get_projects_with_stats(
            batch_size=2,
            rate_limit_delay=0.1,
        )

        # All projects should be processed, but with errors
        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)
        assert all(r.success is False for r in results)
        assert all(r.error is not None for r in results)

    def test_batch_processing_handles_partial_failure(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test batch processing handles some projects failing while others succeed."""
        # Setup mock projects
        mock_projects = [
            self._create_mock_project("project1", "group/project1"),
            self._create_mock_project("project2", "group/project2"),
            self._create_mock_project("project3", "group/project3"),
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Setup mock for projects.get - first succeeds, second fails, third succeeds
        mock_gl_project_success = Mock()
        mock_gl_project_success.commits.list.return_value = []
        mock_gl_project_success.created_at = "2024-01-01T00:00:00Z"

        def get_project_side_effect(project_name):
            if "project2" in project_name:
                raise Exception("Project not found")
            return mock_gl_project_success

        mock_gitlab_instance.projects.get.side_effect = get_project_side_effect

        # Test batch processing
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        results = connector.get_projects_with_stats(
            batch_size=3,
            rate_limit_delay=0.1,
        )

        # All projects should be processed
        assert len(results) == 3

        # Count successes and failures
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]

        assert len(successes) == 2
        assert len(failures) == 1

        # Verify the failed project has the correct error
        failed_result = failures[0]
        assert "Project not found" in str(failed_result.error)
        assert failed_result.repository.full_name == "group/project2"

    @pytest.mark.asyncio
    async def test_async_batch_processing_handles_api_error(
        self, mock_gitlab_client, mock_rest_client
    ):
        """Test async batch processing continues when get_repo_stats raises an exception."""
        # Setup mock projects
        mock_projects = [
            self._create_mock_project("project1", "group/project1"),
            self._create_mock_project("project2", "group/project2"),
        ]

        mock_gitlab_instance = mock_gitlab_client.return_value
        mock_gitlab_instance.projects.list.return_value = mock_projects

        # Setup mock for projects.get to raise an exception
        mock_gitlab_instance.projects.get.side_effect = Exception(
            "API rate limit exceeded"
        )

        # Test async batch processing
        connector = GitLabConnector(
            url="https://gitlab.com", private_token="test_token"
        )
        results = await connector.get_projects_with_stats_async(
            batch_size=2,
            rate_limit_delay=0.1,
        )

        # All projects should be processed, but with errors
        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)
        assert all(r.success is False for r in results)
        assert all(r.error is not None for r in results)
