"""Tests for GitBlame functionality including repo instance reuse."""

import logging
import os
from unittest.mock import MagicMock, patch

from dev_health_ops.models.git import GitBlame, GitBlameMixin


class TestGitBlameMixin:
    """Test cases for GitBlameMixin.fetch_blame()."""

    def test_fetch_blame_without_repo_param(self, repo_path, test_file, repo_uuid):
        """Test fetch_blame creates its own Repo instance when none provided."""
        blame_data = GitBlameMixin.fetch_blame(repo_path, test_file, repo_uuid)

        # Should return a list of blame data tuples
        assert isinstance(blame_data, list)
        # README.md should have content
        if blame_data:
            # Each tuple should have 8 elements
            assert len(blame_data[0]) == 8
            # First element should be the repo_uuid
            assert blame_data[0][0] == repo_uuid

    def test_fetch_blame_with_repo_param(
        self, repo_path, test_file, repo_uuid, git_repo
    ):
        """Test fetch_blame reuses provided Repo instance."""
        blame_data = GitBlameMixin.fetch_blame(
            repo_path, test_file, repo_uuid, repo=git_repo
        )

        # Should return a list of blame data tuples
        assert isinstance(blame_data, list)
        if blame_data:
            # Each tuple should have 8 elements
            assert len(blame_data[0]) == 8
            # First element should be the repo_uuid
            assert blame_data[0][0] == repo_uuid

    def test_fetch_blame_repo_reuse_performance(self, repo_path, repo_uuid, git_repo):
        """Test that passing a repo instance avoids creating a new one."""
        # Mock the Repo class to track instantiation
        with patch("dev_health_ops.models.git.Repo") as mock_repo:
            # Call with existing repo - should not create a new one
            GitBlameMixin.fetch_blame(
                repo_path,
                os.path.join(repo_path, "README.md"),
                repo_uuid,
                repo=git_repo,
            )

            # Repo should not be called when we pass an existing instance
            mock_repo.assert_not_called()

    def test_fetch_blame_creates_repo_when_none_provided(self, repo_path, repo_uuid):
        """Test that fetch_blame creates a GitRepo when none is provided."""
        with patch("dev_health_ops.models.git.GitRepo") as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.blame.return_value = []
            mock_repo.return_value = mock_repo_instance

            GitBlameMixin.fetch_blame(
                repo_path, os.path.join(repo_path, "README.md"), repo_uuid, repo=None
            )

            # GitRepo should be instantiated when none is provided
            mock_repo.assert_called_once_with(repo_path)

    def test_fetch_blame_handles_errors_gracefully(
        self, repo_path, repo_uuid, git_repo, caplog
    ):
        """Test that fetch_blame handles errors and logs warning message."""
        with caplog.at_level(logging.WARNING):
            # Try to process a non-existent file
            blame_data = GitBlameMixin.fetch_blame(
                repo_path,
                os.path.join(repo_path, "nonexistent_file.xyz"),
                repo_uuid,
                repo=git_repo,
            )

            # Should return empty list on error
            assert blame_data == []

            # Should log warning message
            assert "Error processing" in caplog.text


class TestGitBlameProcessFile:
    """Test cases for GitBlame.process_file()."""

    def test_process_file_without_repo_param(self, repo_path, test_file, repo_uuid):
        """Test process_file works without a repo parameter."""
        blame_objects = GitBlame.process_file(repo_path, test_file, repo_uuid)

        assert isinstance(blame_objects, list)
        if blame_objects:
            # Should return GitBlame objects
            assert isinstance(blame_objects[0], GitBlame)
            # Check attributes are set
            assert blame_objects[0].repo_id == repo_uuid
            assert blame_objects[0].line_no >= 1
            assert blame_objects[0].path is not None

    def test_process_file_with_repo_param(
        self, repo_path, test_file, repo_uuid, git_repo
    ):
        """Test process_file reuses provided Repo instance."""
        blame_objects = GitBlame.process_file(
            repo_path, test_file, repo_uuid, repo=git_repo
        )

        assert isinstance(blame_objects, list)
        if blame_objects:
            assert isinstance(blame_objects[0], GitBlame)
            assert blame_objects[0].repo_id == repo_uuid

    def test_process_file_passes_repo_to_fetch_blame(
        self, repo_path, test_file, repo_uuid, git_repo
    ):
        """Test that process_file passes repo parameter to fetch_blame."""
        with patch.object(GitBlame, "fetch_blame") as mock_fetch:
            mock_fetch.return_value = []

            GitBlame.process_file(repo_path, test_file, repo_uuid, repo=git_repo)

            # Verify fetch_blame was called with the repo parameter
            mock_fetch.assert_called_once_with(
                repo_path, test_file, repo_uuid, repo=git_repo
            )

    def test_process_file_returns_correct_gitblame_objects(
        self, repo_path, test_file, repo_uuid, git_repo
    ):
        """Test that process_file correctly maps blame data to GitBlame objects."""
        # Create mock blame data
        mock_blame_data = [
            (
                repo_uuid,
                "test@example.com",
                "Test Author",
                "2024-01-01 00:00:00",
                "abc123",
                1,
                "line content",
                "README.md",
            )
        ]

        with patch.object(GitBlame, "fetch_blame", return_value=mock_blame_data):
            blame_objects = GitBlame.process_file(
                repo_path, test_file, repo_uuid, repo=git_repo
            )

            assert len(blame_objects) == 1
            obj = blame_objects[0]
            assert obj.repo_id == repo_uuid
            assert obj.author_email == "test@example.com"
            assert obj.author_name == "Test Author"
            assert obj.commit_hash == "abc123"
            assert obj.line_no == 1
            assert obj.line == "line content"
            assert obj.path == "README.md"


class TestRepoInstanceReuse:
    """Integration tests for repo instance reuse across multiple files."""

    def test_single_repo_instance_for_multiple_files(
        self, repo_path, repo_uuid, git_repo
    ):
        """Test that a single repo instance can be used for multiple files."""
        # Get a few files from the repo
        test_files = [
            os.path.join(repo_path, "README.md"),
            os.path.join(repo_path, "dev_health_ops.storage.py"),
        ]

        # Process each file with the same repo instance
        all_results = []
        for test_file in test_files:
            if os.path.exists(test_file):
                results = GitBlame.process_file(
                    repo_path, test_file, repo_uuid, repo=git_repo
                )
                all_results.extend(results)

        # Should have processed files successfully
        assert len(all_results) > 0

        # All results should have the same repo_uuid
        for result in all_results:
            assert result.repo_id == repo_uuid
