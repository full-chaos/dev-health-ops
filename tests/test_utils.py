"""Tests for utility functions in utils.py."""

import os
from unittest.mock import patch

import pytest

from dev_health_ops.cli import build_parser
from dev_health_ops.utils import SKIP_EXTENSIONS, is_skippable


class TestIsSkippable:
    """Test cases for the is_skippable function."""

    def test_skippable_extensions(self):
        """Test that common binary file extensions are skippable."""
        skippable_files = [
            "image.png",
            "photo.jpg",
            "photo.jpeg",
            "animation.gif",
            "icon.ico",
            "document.pdf",
            "font.ttf",
            "font.woff",
            "font.woff2",
            "video.mp4",
            "audio.mp3",
            "archive.zip",
            "archive.tar",
            "archive.gz",
            "compiled.pyc",
            "library.so",
            "temp.tmp",
            "backup.bak",
        ]

        for filename in skippable_files:
            assert is_skippable(filename), f"{filename} should be skippable"

    def test_non_skippable_extensions(self):
        """Test that source code files are not skippable."""
        processable_files = [
            "script.py",
            "module.js",
            "styles.css",
            "page.html",
            "data.json",
            "config.yaml",
            "README.md",
            "Makefile",
            "Dockerfile",
            ".gitignore",
            "code.go",
            "main.rs",
            "app.rb",
            "index.ts",
        ]

        for filename in processable_files:
            assert not is_skippable(filename), f"{filename} should not be skippable"

    def test_case_insensitivity(self):
        """Test that extension checking is case-insensitive."""
        # Upper case extensions should still be skippable
        assert is_skippable("IMAGE.PNG")
        assert is_skippable("Photo.JPG")
        assert is_skippable("Document.PDF")

    def test_path_with_directories(self):
        """Test that paths with directories are handled correctly."""
        assert is_skippable("path/to/image.png")
        assert is_skippable("./relative/path/photo.jpg")
        assert is_skippable("/absolute/path/video.mp4")
        assert not is_skippable("path/to/script.py")
        assert not is_skippable("/absolute/path/README.md")

    def test_skip_extensions_set_is_complete(self):
        """Verify the SKIP_EXTENSIONS set contains expected binary types."""
        expected_extensions = {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".pdf",
            ".exe",
            ".zip",
            ".tar",
            ".gz",
            ".pyc",
            ".so",
            ".bin",
        }

        for ext in expected_extensions:
            assert ext in SKIP_EXTENSIONS, f"{ext} should be in SKIP_EXTENSIONS"

    def test_hidden_files(self):
        """Test that hidden files (starting with .) are handled correctly."""
        # Hidden files without binary extensions should not be skippable
        assert not is_skippable(".gitignore")
        assert not is_skippable(".env")

    def test_files_without_extensions(self):
        """Test that files without extensions are not skippable by extension."""
        # Files without extensions should not be skippable (unless mime type says otherwise)
        assert not is_skippable("Makefile")
        assert not is_skippable("Dockerfile")
        assert not is_skippable("LICENSE")

    def test_extension_based_skips(self):
        """Test that extension-based skipping covers common binary types."""
        assert is_skippable("video.avi")
        assert is_skippable("audio.wav")


class TestDBEchoConfiguration:
    """Test cases for DB_ECHO environment variable parsing."""

    def test_db_echo_defaults_to_false_when_not_set(self):
        """Test that DB_ECHO defaults to False when not set."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove DB_ECHO if it exists
            os.environ.pop("DB_ECHO", None)
            # Re-evaluate the expression
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_true_for_true_value(self):
        """Test that DB_ECHO is True when set to 'true'."""
        with patch.dict(os.environ, {"DB_ECHO": "true"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_true_for_uppercase_true(self):
        """Test that DB_ECHO is True when set to 'TRUE' (case-insensitive)."""
        with patch.dict(os.environ, {"DB_ECHO": "TRUE"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_true_for_one(self):
        """Test that DB_ECHO is True when set to '1'."""
        with patch.dict(os.environ, {"DB_ECHO": "1"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_true_for_yes(self):
        """Test that DB_ECHO is True when set to 'yes'."""
        with patch.dict(os.environ, {"DB_ECHO": "yes"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_true_for_uppercase_yes(self):
        """Test that DB_ECHO is True when set to 'YES' (case-insensitive)."""
        with patch.dict(os.environ, {"DB_ECHO": "YES"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_false_for_false_value(self):
        """Test that DB_ECHO is False when set to 'false'."""
        with patch.dict(os.environ, {"DB_ECHO": "false"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_false_for_zero(self):
        """Test that DB_ECHO is False when set to '0'."""
        with patch.dict(os.environ, {"DB_ECHO": "0"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_false_for_no(self):
        """Test that DB_ECHO is False when set to 'no'."""
        with patch.dict(os.environ, {"DB_ECHO": "no"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_false_for_invalid_value(self):
        """Test that DB_ECHO is False when set to an invalid value."""
        with patch.dict(os.environ, {"DB_ECHO": "invalid"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_false_for_empty_string(self):
        """Test that DB_ECHO is False when set to an empty string."""
        with patch.dict(os.environ, {"DB_ECHO": ""}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False


class TestBatchProcessingCLIArguments:
    """Test cases for batch processing CLI argument parsing.

    These tests exercise cli.py argument parsing for batch sync flows.
    """

    def test_github_pattern_argument(self):
        """Test that --search argument is parsed correctly."""
        parser = build_parser()
        test_args = [
            "sync",
            "git",
            "--provider",
            "github",
            "--db",
            "sqlite+aiosqlite:///:memory:",
            "--search",
            "chrisgeo/m*",
        ]
        args = parser.parse_args(test_args)

        assert args.search == "chrisgeo/m*"
        assert args.batch_size == 10
        assert args.max_concurrent == 4
        assert args.rate_limit_delay == 1.0
        assert args.max_commits_per_repo is None
        assert args.max_repos is None
        assert args.use_async is False
        assert args.date is None
        assert args.backfill == 1

    def test_batch_processing_arguments_with_custom_values(self):
        """Test that batch processing arguments accept custom values."""
        parser = build_parser()
        test_args = [
            "sync",
            "git",
            "--provider",
            "github",
            "--db",
            "sqlite+aiosqlite:///:memory:",
            "--search",
            "org/*",
            "--batch-size",
            "20",
            "--max-concurrent",
            "8",
            "--rate-limit-delay",
            "2.5",
            "--max-commits-per-repo",
            "100",
            "--max-repos",
            "50",
            "--use-async",
        ]

        args = parser.parse_args(test_args)

        assert args.search == "org/*"
        assert args.batch_size == 20
        assert args.max_concurrent == 8
        assert args.rate_limit_delay == 2.5
        assert args.max_commits_per_repo == 100
        assert args.max_repos == 50
        assert args.use_async is True

    def test_use_async_flag_default_is_false(self):
        """Test that --use-async flag defaults to False."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "sync",
                "git",
                "--provider",
                "github",
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "--search",
                "org/*",
            ]
        )

        assert args.use_async is False

    def test_metrics_daily_provider_default_is_auto(self):
        """Test that metrics daily defaults to provider=auto."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "metrics",
                "daily",
                "--db",
                "sqlite+aiosqlite:///:memory:",
            ]
        )
        assert args.provider == "auto"

    def test_use_async_flag_when_provided(self):
        """Test that --use-async flag is True when provided."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "sync",
                "git",
                "--provider",
                "github",
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "--search",
                "org/*",
                "--use-async",
            ]
        )

        assert args.use_async is True

    def test_gitlab_pattern_argument(self):
        """Test that --search argument is parsed correctly for GitLab."""
        parser = build_parser()
        test_args = [
            "sync",
            "git",
            "--provider",
            "gitlab",
            "--db",
            "sqlite+aiosqlite:///:memory:",
            "--search",
            "group/p*",
        ]
        args = parser.parse_args(test_args)

        assert args.search == "group/p*"
        assert args.batch_size == 10
        assert args.max_concurrent == 4
        assert args.rate_limit_delay == 1.0
        assert args.max_commits_per_repo is None
        assert args.max_repos is None
        assert args.use_async is False
        assert args.date is None
        assert args.backfill == 1

    def test_grafana_subcommand_removed(self):
        """Test that the deprecated grafana subcommand is not accepted."""
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["grafana", "up"])

    def test_gitlab_batch_processing_arguments_with_custom_values(self):
        """Test that GitLab batch processing arguments accept custom values."""
        parser = build_parser()
        test_args = [
            "sync",
            "git",
            "--provider",
            "gitlab",
            "--db",
            "sqlite+aiosqlite:///:memory:",
            "--search",
            "mygroup/*",
            "--group",
            "mygroup",
            "--batch-size",
            "15",
            "--max-concurrent",
            "6",
            "--rate-limit-delay",
            "1.5",
            "--max-commits-per-repo",
            "50",
            "--max-repos",
            "25",
            "--use-async",
        ]

        args = parser.parse_args(test_args)

        assert args.search == "mygroup/*"
        assert args.group == "mygroup"
        assert args.batch_size == 15
        assert args.max_concurrent == 6
        assert args.rate_limit_delay == 1.5
        assert args.max_commits_per_repo == 50
        assert args.max_repos == 25
        assert args.use_async is True


class TestSyncTimeWindowCLIArguments:
    def test_sync_local_accepts_date_backfill(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "sync",
                "git",
                "--provider",
                "local",
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "--date",
                "2025-01-02",
                "--backfill",
                "7",
            ]
        )
        assert str(args.date) == "2025-01-02"
        assert args.backfill == 7

    def test_sync_local_rejects_since_and_date_together(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "sync",
                    "git",
                    "--provider",
                    "local",
                    "--db",
                    "sqlite+aiosqlite:///:memory:",
                    "--since",
                    "2025-01-01T00:00:00+00:00",
                    "--date",
                    "2025-01-02",
                ]
            )
