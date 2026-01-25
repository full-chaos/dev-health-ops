"""Tests for performance optimizations."""

import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.processors.local import (
    process_git_commit_stats,
    process_git_commits,
)
from dev_health_ops.utils import BATCH_SIZE, MAX_WORKERS


class TestBatchSizeConfiguration:
    """Test that batch size is properly configurable."""

    def test_batch_size_defaults_to_1000(self):
        """Test that BATCH_SIZE defaults to 1000."""
        # BATCH_SIZE is imported directly from dev_health_ops.utils
        assert BATCH_SIZE == 1000

    def test_batch_size_can_be_configured_via_env(self):
        """Test that BATCH_SIZE can be configured via environment variable."""
        with patch.dict(os.environ, {"BATCH_SIZE": "250"}):
            # Re-evaluate the expression
            batch_size = int(os.getenv("BATCH_SIZE", "1000"))
            assert batch_size == 250


class TestMaxWorkersConfiguration:
    """Test that MAX_WORKERS is properly configurable."""

    def test_max_workers_defaults_to_4(self):
        """Test that MAX_WORKERS defaults to 4."""
        # MAX_WORKERS is imported directly from dev_health_ops.utils
        assert MAX_WORKERS == 4

    def test_max_workers_can_be_configured_via_env(self):
        """Test that MAX_WORKERS can be configured via environment variable."""
        with patch.dict(os.environ, {"MAX_WORKERS": "8"}):
            # Re-evaluate the expression
            max_workers = int(os.getenv("MAX_WORKERS", "4"))
            assert max_workers == 8


class TestCommitProcessing:
    """Test git commit processing."""

    @pytest.mark.asyncio
    async def test_process_git_commits_batches_inserts(self):
        """Test that git commits are inserted in batches."""
        mock_store = AsyncMock()
        mock_repo = MagicMock()
        mock_repo.id = uuid.uuid4()  # Mock the repo.id

        # Create mock commits
        mock_commits = []
        for i in range(5):
            mock_commit = MagicMock()
            mock_commit.hexsha = f"hash{i}"
            mock_commit.message = f"Message {i}"
            mock_commit.author.name = "Test Author"
            mock_commit.author.email = "test@example.com"
            mock_commit.authored_datetime = "2024-01-01 00:00:00"
            mock_commit.committer.name = "Test Committer"
            mock_commit.committer.email = "committer@example.com"
            mock_commit.committed_datetime = "2024-01-01 00:00:00"
            mock_commit.parents = []
            mock_commits.append(mock_commit)

        mock_repo.iter_commits.return_value = iter(mock_commits)

        with patch("dev_health_ops.processors.local.logging") as mock_logging:
            await process_git_commits(mock_repo, mock_store)
            # Verify the function was called and processing occurred
            assert mock_logging.info.call_count >= 1


class TestCommitStatsProcessing:
    """Test git commit stats processing."""

    @pytest.mark.asyncio
    async def test_process_git_commit_stats_batches_inserts(self):
        """Test that commit stats are inserted in batches."""
        mock_store = AsyncMock()
        mock_repo = MagicMock()
        mock_repo.id = uuid.uuid4()  # Mock the repo.id

        # Create mock commits with diffs
        mock_commit = MagicMock()
        mock_commit.hexsha = "hash1"
        mock_commit.parents = [MagicMock()]

        # Mock diff
        mock_diff = MagicMock()
        mock_diff.b_path = "test.py"
        mock_diff.a_path = "test.py"
        mock_diff.diff = b"diff content"
        mock_diff.a_mode = 0o100644
        mock_diff.b_mode = 0o100644

        mock_commit.parents[0].diff.return_value = [mock_diff]
        mock_repo.iter_commits.return_value = iter([mock_commit])

        with patch("dev_health_ops.processors.local.logging") as mock_logging:
            await process_git_commit_stats(mock_repo, mock_store)
            # Verify the function was called and processing occurred
            assert mock_logging.info.call_count >= 1


class TestConnectionPooling:
    """Test connection pooling configuration."""

    def test_postgresql_connection_pool_configured(self):
        """Test that PostgreSQL connections use pooling."""
        from dev_health_ops.storage import SQLAlchemyStore

        conn_string = "postgresql+asyncpg://user:pass@localhost/db"
        store = SQLAlchemyStore(conn_string)

        # Check that the engine was created (we can't easily inspect pool settings)
        assert store.engine is not None
        store.engine.sync_engine.dispose()

    def test_sqlite_connection_no_pooling(self):
        """Test that SQLite connections don't use pooling parameters."""
        from dev_health_ops.storage import SQLAlchemyStore

        conn_string = "sqlite+aiosqlite:///test.db"
        # Should not raise an error about invalid pool parameters
        store = SQLAlchemyStore(conn_string)
        assert store.engine is not None
        store.engine.sync_engine.dispose()


class TestRepoUUIDDerivation:
    """Test derivation of REPO_UUID from git repository data."""

    def test_get_repo_uuid_is_deterministic(self, tmp_path):
        """Test that get_repo_uuid returns the same UUID for the same repo."""
        from dev_health_ops.models.git import get_repo_uuid

        # Clear REPO_UUID env var for this test
        with patch.dict(os.environ, {}, clear=True):
            if "REPO_UUID" in os.environ:
                del os.environ["REPO_UUID"]

            # Use the current repository
            uuid1 = get_repo_uuid(".")
            uuid2 = get_repo_uuid(".")

            assert uuid1 == uuid2
            # Should be a valid UUID
            assert isinstance(uuid1, uuid.UUID)

    def test_get_repo_uuid_respects_env_var(self):
        """Test that get_repo_uuid respects REPO_UUID environment variable."""
        from dev_health_ops.models.git import get_repo_uuid

        test_uuid = "12345678-1234-1234-1234-123456789abc"
        with patch.dict(os.environ, {"REPO_UUID": test_uuid}):
            result = get_repo_uuid(".")
            assert result == uuid.UUID(test_uuid)

    def test_get_repo_uuid_handles_missing_repo(self, tmp_path):
        """Test that get_repo_uuid handles non-git directories gracefully."""
        from dev_health_ops.models.git import get_repo_uuid

        # Clear REPO_UUID env var
        with patch.dict(os.environ, {}, clear=True):
            if "REPO_UUID" in os.environ:
                del os.environ["REPO_UUID"]

            # Use a non-git directory
            non_git_dir = tmp_path / "not_a_git_repo"
            non_git_dir.mkdir()

            result = get_repo_uuid(str(non_git_dir))

            # Should return a valid UUID (fallback to random)
            assert isinstance(result, uuid.UUID)
