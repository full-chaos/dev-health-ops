"""
Tests for the base connector class.
"""

import pytest

from dev_health_ops.connectors import GitConnector
from dev_health_ops.connectors.base import BatchResult
from dev_health_ops.connectors.models import Repository, RepoStats


class TestBatchResult:
    """Tests for the BatchResult dataclass."""

    def test_successful_result(self):
        """Test creating a successful batch result."""
        repo = Repository(
            id=1,
            name="test-repo",
            full_name="owner/test-repo",
            default_branch="main",
        )
        stats = RepoStats(
            total_commits=10,
            additions=100,
            deletions=50,
            commits_per_week=2.0,
            authors=[],
        )
        result = BatchResult(repository=repo, stats=stats, success=True)

        assert result.success is True
        assert result.repository == repo
        assert result.stats == stats
        assert result.error is None

    def test_failed_result(self):
        """Test creating a failed batch result."""
        repo = Repository(
            id=1,
            name="test-repo",
            full_name="owner/test-repo",
            default_branch="main",
        )
        result = BatchResult(
            repository=repo,
            error="API error",
            success=False,
        )

        assert result.success is False
        assert result.repository == repo
        assert result.stats is None
        assert result.error == "API error"


class TestGitConnectorInterface:
    """Tests for the GitConnector abstract base class."""

    def test_cannot_instantiate_base_class(self):
        """Test that the base class cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            GitConnector()

    def test_concrete_class_must_implement_abstract_methods(self):
        """Test that concrete class must implement all abstract methods."""

        class IncompleteConnector(GitConnector):
            """A connector that doesn't implement all abstract methods."""

            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteConnector()

    def test_concrete_class_with_all_methods(self):
        """Test that a concrete class can be instantiated when all methods are implemented."""

        class ConcreteConnector(GitConnector):
            """A complete connector implementation."""

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(
                self,
                org_name=None,
                user_name=None,
                search=None,
                pattern=None,
                max_repos=None,
            ):
                return []

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                return None

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def get_repos_with_stats(
                self,
                org_name=None,
                user_name=None,
                pattern=None,
                batch_size=10,
                max_concurrent=4,
                rate_limit_delay=1.0,
                max_commits_per_repo=None,
                max_repos=None,
                on_repo_complete=None,
            ):
                return []

            async def get_repos_with_stats_async(
                self,
                org_name=None,
                user_name=None,
                pattern=None,
                batch_size=10,
                max_concurrent=4,
                rate_limit_delay=1.0,
                max_commits_per_repo=None,
                max_repos=None,
                on_repo_complete=None,
            ):
                return []

            def close(self):
                pass

        connector = ConcreteConnector()
        assert connector.per_page == 100
        assert connector.max_workers == 4

    def test_custom_per_page_and_max_workers(self):
        """Test that custom per_page and max_workers can be set."""

        class ConcreteConnector(GitConnector):
            """A complete connector implementation."""

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(
                self,
                org_name=None,
                user_name=None,
                search=None,
                pattern=None,
                max_repos=None,
            ):
                return []

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                return None

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def get_repos_with_stats(
                self,
                org_name=None,
                user_name=None,
                pattern=None,
                batch_size=10,
                max_concurrent=4,
                rate_limit_delay=1.0,
                max_commits_per_repo=None,
                max_repos=None,
                on_repo_complete=None,
            ):
                return []

            async def get_repos_with_stats_async(
                self,
                org_name=None,
                user_name=None,
                pattern=None,
                batch_size=10,
                max_concurrent=4,
                rate_limit_delay=1.0,
                max_commits_per_repo=None,
                max_repos=None,
                on_repo_complete=None,
            ):
                return []

            def close(self):
                pass

        connector = ConcreteConnector(per_page=50, max_workers=8)
        assert connector.per_page == 50
        assert connector.max_workers == 8


class TestGitConnectorContextManager:
    """Tests for the context manager protocol."""

    def test_context_manager(self):
        """Test that the connector can be used as a context manager."""

        class TestConnector(GitConnector):
            """Test connector implementation."""

            def __init__(self):
                super().__init__()
                self.closed = False

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(
                self,
                org_name=None,
                user_name=None,
                search=None,
                pattern=None,
                max_repos=None,
            ):
                return []

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                return None

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def get_repos_with_stats(
                self,
                org_name=None,
                user_name=None,
                pattern=None,
                batch_size=10,
                max_concurrent=4,
                rate_limit_delay=1.0,
                max_commits_per_repo=None,
                max_repos=None,
                on_repo_complete=None,
            ):
                return []

            async def get_repos_with_stats_async(
                self,
                org_name=None,
                user_name=None,
                pattern=None,
                batch_size=10,
                max_concurrent=4,
                rate_limit_delay=1.0,
                max_commits_per_repo=None,
                max_repos=None,
                on_repo_complete=None,
            ):
                return []

            def close(self):
                self.closed = True

        with TestConnector() as connector:
            assert connector.closed is False

        assert connector.closed is True
