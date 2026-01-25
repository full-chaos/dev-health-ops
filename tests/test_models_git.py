"""Tests for models.git timezone-aware datetime functionality."""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from dev_health_ops.models.git import (
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitFile,
    GitRef,
    Repo,
    get_repo_uuid,
    get_repo_uuid_from_repo,
)


class TestRepoUUID:
    """Test that Repo objects get deterministic UUIDs based on git data."""

    def test_two_repos_with_different_remotes_get_different_uuids(self):
        """Test that two repos with different remote URLs get different UUIDs."""
        with patch("dev_health_ops.models.git.GitRepo") as MockGitRepo:
            # Mock first repo with remote URL 1
            mock_repo1 = MagicMock()
            mock_remote1 = MagicMock()
            mock_remote1.name = "origin"
            mock_remote1.urls = iter(["https://github.com/user/repo1.git"])
            mock_repo1.remotes = [mock_remote1]
            mock_repo1.remote.return_value = mock_remote1

            # Mock second repo with remote URL 2
            mock_repo2 = MagicMock()
            mock_remote2 = MagicMock()
            mock_remote2.name = "origin"
            mock_remote2.urls = iter(["https://github.com/user/repo2.git"])
            mock_repo2.remotes = [mock_remote2]
            mock_repo2.remote.return_value = mock_remote2

            MockGitRepo.side_effect = [mock_repo1, mock_repo2]

            uuid1 = get_repo_uuid("/path/to/repo1")
            uuid2 = get_repo_uuid("/path/to/repo2")

            assert uuid1 != uuid2
            assert isinstance(uuid1, uuid.UUID)
            assert isinstance(uuid2, uuid.UUID)

    def test_same_remote_url_produces_same_uuid(self):
        """Test that the same remote URL always produces the same UUID."""
        with patch("dev_health_ops.models.git.GitRepo") as MockGitRepo:

            def create_mock_repo():
                mock_repo = MagicMock()
                mock_remote = MagicMock()
                mock_remote.name = "origin"
                mock_remote.urls = iter(["https://github.com/user/same-repo.git"])
                mock_repo.remotes = [mock_remote]
                mock_repo.remote.return_value = mock_remote
                return mock_repo

            MockGitRepo.side_effect = [create_mock_repo(), create_mock_repo()]

            uuid1 = get_repo_uuid("/path/to/repo")
            uuid2 = get_repo_uuid("/different/path/to/repo")

            assert uuid1 == uuid2

    def test_repo_without_remote_uses_path(self):
        """Test that repos without remotes use absolute path for UUID."""
        with patch("dev_health_ops.models.git.GitRepo") as MockGitRepo:
            mock_repo = MagicMock()
            mock_repo.remotes = []  # No remotes

            MockGitRepo.return_value = mock_repo

            with patch("os.path.abspath") as mock_abspath:
                mock_abspath.return_value = "/absolute/path/to/repo"
                result = get_repo_uuid("/path/to/repo")

            assert isinstance(result, uuid.UUID)

    def test_repo_id_is_set_on_init(self):
        """Test that Repo.id is automatically set when initialized with a path."""
        with (
            patch("dev_health_ops.models.git.get_repo_uuid") as mock_get_uuid,
            patch("dev_health_ops.models.git.GitRepo.__init__", return_value=None),
        ):
            expected_uuid = uuid.uuid4()
            mock_get_uuid.return_value = expected_uuid

            repo = Repo("/path/to/repo")

            assert repo.id == expected_uuid
            mock_get_uuid.assert_called_once_with("/path/to/repo")

    def test_repo_id_not_overwritten_if_provided(self):
        """Test that explicitly provided id is not overwritten."""
        with (
            patch("dev_health_ops.models.git.get_repo_uuid") as mock_get_uuid,
            patch("dev_health_ops.models.git.GitRepo.__init__", return_value=None),
        ):
            explicit_uuid = uuid.uuid4()

            repo = Repo("/path/to/repo", id=explicit_uuid)

            assert repo.id == explicit_uuid
            mock_get_uuid.assert_not_called()

    def test_repo_id_is_set_from_repo_identifier(self):
        """Repo.id should be set when initialized with repo=... (no repo_path)."""
        repo_name = "group/project"
        repo = Repo(repo=repo_name, settings={}, tags=[])

        assert repo.id == get_repo_uuid_from_repo(repo_name)
        assert isinstance(repo.id, uuid.UUID)

    def test_repo_id_from_repo_identifier_is_deterministic(self):
        """Same repo identifier should yield same UUID across instances."""
        repo_name = "group/project"
        repo1 = Repo(repo=repo_name, settings={}, tags=[])
        repo2 = Repo(repo=repo_name, settings={}, tags=[])

        assert repo1.id == repo2.id

    def test_repo_id_from_repo_identifier_is_unique_per_repo(self):
        """Different repo identifiers should yield different UUIDs."""
        repo1 = Repo(repo="group/project1", settings={}, tags=[])
        repo2 = Repo(repo="group/project2", settings={}, tags=[])

        assert repo1.id != repo2.id

    def test_git_commit_uses_repo_id(self):
        """Test that GitCommit can be created with repo.id."""
        with (
            patch("dev_health_ops.models.git.get_repo_uuid") as mock_get_uuid,
            patch("dev_health_ops.models.git.GitRepo.__init__", return_value=None),
        ):
            repo_uuid = uuid.uuid4()
            mock_get_uuid.return_value = repo_uuid

            repo = Repo("/path/to/repo")
            commit = GitCommit(
                repo_id=repo.id,
                hash="abc123",
                message="Test commit",
            )

            assert commit.repo_id == repo_uuid

    def test_two_repos_commits_have_different_repo_ids(self):
        """Test that commits from different repos have different repo_ids."""
        # Create two different UUIDs (simulating two different repos)
        uuid1 = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/user/repo1.git")
        uuid2 = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/user/repo2.git")

        commit1 = GitCommit(repo_id=uuid1, hash="abc123", message="Commit 1")
        commit2 = GitCommit(repo_id=uuid2, hash="def456", message="Commit 2")

        assert commit1.repo_id != commit2.repo_id
        assert commit1.repo_id == uuid1
        assert commit2.repo_id == uuid2


class TestTimezoneAwareDatetimes:
    """Test that all datetime defaults are timezone-aware."""

    def test_repo_created_at_is_timezone_aware(self):
        """Test that Repo.created_at default is timezone-aware."""
        repo = Repo()
        # Get the default value by calling the lambda with a mock context
        default_func = repo.__table__.columns["created_at"].default.arg
        created_at = default_func(MagicMock())

        assert created_at.tzinfo is not None
        assert created_at.tzinfo == timezone.utc

    def test_git_ref_synced_at_is_timezone_aware(self):
        """Test that GitRef.last_synced default is timezone-aware."""
        git_ref = GitRef()
        default_func = git_ref.__table__.columns["last_synced"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc

    def test_git_file_synced_at_is_timezone_aware(self):
        """Test that GitFile.last_synced default is timezone-aware."""
        git_file = GitFile()
        default_func = git_file.__table__.columns["last_synced"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc

    def test_git_commit_synced_at_is_timezone_aware(self):
        """Test that GitCommit.last_synced default is timezone-aware."""
        git_commit = GitCommit()
        default_func = git_commit.__table__.columns["last_synced"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc

    def test_git_commit_stat_synced_at_is_timezone_aware(self):
        """Test that GitCommitStat.last_synced default is timezone-aware."""
        git_commit_stat = GitCommitStat()
        default_func = git_commit_stat.__table__.columns["last_synced"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc

    def test_git_blame_synced_at_is_timezone_aware(self):
        """Test that GitBlame.last_synced default is timezone-aware."""
        git_blame = GitBlame()
        default_func = git_blame.__table__.columns["last_synced"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc


class TestDatetimeDefaultsReturnCurrentTime:
    """Test that datetime defaults return current time, not a fixed time."""

    def test_multiple_repo_creations_have_different_timestamps(self):
        """Test that creating multiple Repo instances generates different timestamps."""
        with patch("dev_health_ops.models.git.datetime") as mock_datetime:
            # Mock two different times
            time1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            time2 = datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
            mock_datetime.now.side_effect = [time1, time2]

            default_func = Repo.__table__.columns["created_at"].default.arg

            timestamp1 = default_func(MagicMock())
            timestamp2 = default_func(MagicMock())

            # Timestamps should be different because datetime.now was called twice
            assert timestamp1 != timestamp2
            assert mock_datetime.now.call_count == 2

    def test_datetime_now_called_with_timezone_utc(self):
        """Test that datetime.now is called with timezone.utc argument."""
        with patch("dev_health_ops.models.git.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(
                2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc
            )

            default_func = Repo.__table__.columns["created_at"].default.arg
            default_func(MagicMock())

            # Verify datetime.now was called with timezone.utc
            mock_datetime.now.assert_called_once_with(timezone.utc)


class TestBackwardCompatibility:
    """Test that the changes maintain backward compatibility."""

    def test_all_models_have_datetime_columns_with_timezone(self):
        """Test that all datetime columns are configured with timezone=True."""
        models_and_columns = [
            (Repo, "created_at"),
            (GitRef, "last_synced"),
            (GitFile, "last_synced"),
            (GitCommit, "last_synced"),
            (GitCommitStat, "last_synced"),
            (GitBlame, "last_synced"),
        ]

        for model, column_name in models_and_columns:
            column = model.__table__.columns[column_name]
            # Check that the column type has timezone=True
            assert column.type.timezone is True, (
                f"{model.__name__}.{column_name} should have timezone=True"
            )

    def test_datetime_defaults_are_callable(self):
        """Test that all datetime defaults are callable (lambdas)."""
        models_and_columns = [
            (Repo, "created_at"),
            (GitRef, "last_synced"),
            (GitFile, "last_synced"),
            (GitCommit, "last_synced"),
            (GitCommitStat, "last_synced"),
            (GitBlame, "last_synced"),
        ]

        for model, column_name in models_and_columns:
            column = model.__table__.columns[column_name]
            assert column.default is not None, (
                f"{model.__name__}.{column_name} should have a default"
            )
            assert callable(column.default.arg), (
                f"{model.__name__}.{column_name} default should be callable"
            )
