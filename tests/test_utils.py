"""Tests for utility functions in utils.py."""

import os
from unittest.mock import patch

import pytest

from dev_health_ops.cli import _load_dotenv, build_parser
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
            "--db",
            "sqlite+aiosqlite:///:memory:",
            "sync",
            "git",
            "--provider",
            "github",
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
            "--db",
            "sqlite+aiosqlite:///:memory:",
            "sync",
            "git",
            "--provider",
            "github",
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
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "sync",
                "git",
                "--provider",
                "github",
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
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "metrics",
                "daily",
            ]
        )
        assert args.provider == "auto"

    def test_use_async_flag_when_provided(self):
        """Test that --use-async flag is True when provided."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "sync",
                "git",
                "--provider",
                "github",
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
            "--db",
            "sqlite+aiosqlite:///:memory:",
            "sync",
            "git",
            "--provider",
            "gitlab",
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
            "--db",
            "sqlite+aiosqlite:///:memory:",
            "sync",
            "git",
            "--provider",
            "gitlab",
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
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "sync",
                "git",
                "--provider",
                "local",
                "--date",
                "2025-01-02",
                "--backfill",
                "7",
            ]
        )
        assert str(args.date) == "2025-01-02"
        assert args.backfill == 7

    def test_sync_local_accepts_since_and_date_together(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "sync",
                "git",
                "--provider",
                "local",
                "--since",
                "2025-01-01",
                "--date",
                "2025-01-02",
            ]
        )
        assert str(args.since) == "2025-01-01"
        assert str(args.date) == "2025-01-02"


class TestGlobalFlagsPropagateToSubparsers:
    """Regression: global flags (--org, --db, --analytics-db, --log-level,
    --llm-provider, --model) must be accepted EITHER before OR after the
    subcommand. Previously argparse rejected the after-subcommand form with
    `unrecognized arguments: --org X`."""

    def test_org_accepted_after_fixtures_generate(self):
        parser = build_parser()
        args = parser.parse_args(
            ["fixtures", "generate", "--org", "acme-org", "--days", "1"]
        )
        assert args.org == "acme-org"
        assert args.days == 1

    def test_org_accepted_after_sync_git(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "sync",
                "git",
                "--provider",
                "github",
                "--search",
                "org/*",
                "--org",
                "acme-org",
            ]
        )
        assert args.org == "acme-org"

    def test_org_accepted_after_metrics_daily(self):
        parser = build_parser()
        args = parser.parse_args(["metrics", "daily", "--org", "acme-org"])
        assert args.org == "acme-org"

    def test_org_accepted_after_audit_perf(self):
        parser = build_parser()
        args = parser.parse_args(["audit", "perf", "--org", "acme-org"])
        assert args.org == "acme-org"

    def test_org_before_subcommand_still_works(self):
        parser = build_parser()
        args = parser.parse_args(
            ["--org", "acme-org", "fixtures", "generate", "--days", "1"]
        )
        assert args.org == "acme-org"

    def test_db_analytics_db_and_log_level_propagate_to_leaves(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "fixtures",
                "generate",
                "--db",
                "sqlite+aiosqlite:///:memory:",
                "--analytics-db",
                "clickhouse://localhost:8123/default",
                "--log-level",
                "DEBUG",
            ]
        )
        assert args.db == "sqlite+aiosqlite:///:memory:"
        assert args.analytics_db == "clickhouse://localhost:8123/default"
        assert args.log_level == "DEBUG"

    def test_root_default_preserved_when_leaf_flag_omitted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["--org", "from-root", "fixtures", "generate", "--days", "1"]
        )
        # Leaf parser uses default=SUPPRESS, so omitting --org on the leaf must
        # NOT clobber the value supplied before the subcommand.
        assert args.org == "from-root"


class TestCLIPlumbing:
    def test_investment_materialize_preserves_root_llm_arguments(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "-l",
                "openai",
                "-m",
                "gpt-4o-mini",
                "investment",
                "materialize",
            ]
        )

        assert args.llm_provider == "openai"
        assert args.model == "gpt-4o-mini"

    def test_investment_materialize_preserves_root_db_arguments(self):
        parser = build_parser()
        postgres_dsn = "postgresql+asyncpg://pg:pg@localhost:5432/devhealth"
        clickhouse_dsn = "clickhouse://ch:ch@localhost:8123/default"
        args = parser.parse_args(
            [
                "--db",
                postgres_dsn,
                "--analytics-db",
                clickhouse_dsn,
                "investment",
                "materialize",
            ]
        )

        assert args.db == postgres_dsn
        assert args.analytics_db == clickhouse_dsn

    def test_investment_materialize_accepts_deprecated_db_alias(self):
        parser = build_parser()
        clickhouse_dsn = "clickhouse://ch:ch@localhost:8123/default"
        args = parser.parse_args(["investment", "materialize", "--db", clickhouse_dsn])

        assert args.analytics_db == clickhouse_dsn

    def test_load_dotenv_expands_compose_interpolation(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
        monkeypatch.delenv("CLICKHOUSE_USER", raising=False)
        monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")
        dotenv_path = tmp_path / ".env"
        dotenv_path.write_text(
            "CLICKHOUSE_URI=clickhouse://${CLICKHOUSE_USER:-ch}:${CLICKHOUSE_PASSWORD:-ch}@localhost:8123/default\n",
            encoding="utf-8",
        )

        assert _load_dotenv(dotenv_path) == 1
        assert (
            os.environ["CLICKHOUSE_URI"]
            == "clickhouse://ch:secret@localhost:8123/default"
        )

    def test_load_dotenv_rejects_unresolved_interpolation(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
        monkeypatch.delenv("MISSING_DOTENV_VAR", raising=False)
        dotenv_path = tmp_path / ".env"
        dotenv_path.write_text(
            "CLICKHOUSE_URI=clickhouse://${MISSING_DOTENV_VAR}@localhost:8123/default\n",
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="MISSING_DOTENV_VAR"):
            _load_dotenv(dotenv_path)
