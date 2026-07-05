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
            GitConnector()  # type: ignore[abstract]

    def test_concrete_class_must_implement_abstract_methods(self):
        """Test that concrete class must implement all abstract methods."""

        class IncompleteConnector(GitConnector):
            """A connector that doesn't implement all abstract methods."""

            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteConnector()  # type: ignore[abstract]

    def test_concrete_class_with_all_methods(self):
        """Test that a concrete class can be instantiated when all methods are implemented."""

        class ConcreteConnector(GitConnector):
            """A complete connector implementation."""

            def close(self):
                pass

        connector = ConcreteConnector()
        assert connector.per_page == 100
        assert connector.max_workers == 4

    def test_custom_per_page_and_max_workers(self):
        """Test that custom per_page and max_workers can be set."""

        class ConcreteConnector(GitConnector):
            """A complete connector implementation."""

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

            def close(self):
                self.closed = True

        with TestConnector() as connector:
            assert connector.closed is False

        assert connector.closed is True
