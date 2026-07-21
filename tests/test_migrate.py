"""Tests for the dev-hops migrate CLI command."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import patch

# Force-import the alembic.command submodule so `@patch("alembic.command")` in
# TestCommandDispatch resolves regardless of test execution order. The submodule
# is only lazily imported inside dev_health_ops.migrate functions, so without
# this the patch target is missing when the module runs first on an xdist worker
# or in isolation (CHAOS-2586).
import alembic.command  # noqa: F401
import pytest

from dev_health_ops import migrate as migrate_mod
from dev_health_ops.migrate import (
    _ALEMBIC_DIR,
    _get_migration_database_uri,
    _make_alembic_config,
    _run_current,
    _run_downgrade,
    _run_heads,
    _run_history,
    _run_river_upgrade,
    _run_upgrade,
    register_commands,
)

# ── alembic directory resolution ───────────────────────────────────


class TestAlembicDirResolution:
    def test_alembic_dir_exists(self):
        assert _ALEMBIC_DIR.is_dir(), f"Expected alembic dir at {_ALEMBIC_DIR}"

    def test_alembic_dir_contains_env_py(self):
        assert (_ALEMBIC_DIR / "env.py").is_file()

    def test_alembic_dir_contains_versions(self):
        versions = _ALEMBIC_DIR / "versions"
        assert versions.is_dir()
        migration_files = list(versions.glob("*.py"))
        # Exclude __init__.py
        migration_files = [f for f in migration_files if f.name != "__init__.py"]
        assert len(migration_files) > 0, "No migration files found in versions/"

    def test_alembic_dir_is_relative_to_package(self):
        """The dir must resolve from the installed package, not a hardcoded path."""
        package_dir = Path(migrate_mod.__file__).resolve().parent
        assert package_dir / "alembic" == _ALEMBIC_DIR


# ── _make_alembic_config ───────────────────────────────────────────


class TestMakeAlembicConfig:
    def test_sets_script_location(self):
        cfg = _make_alembic_config(db_url="sqlite:///test.db")
        assert cfg.get_main_option("script_location") == str(_ALEMBIC_DIR)

    def test_explicit_db_url(self):
        url = "postgresql+asyncpg://user:pass@host/db"
        cfg = _make_alembic_config(db_url=url)
        assert cfg.get_main_option("sqlalchemy.url") == url

    def test_migration_database_uri_precedes_runtime_uri(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_DATABASE_URI", "postgresql://migration@direct/db")
        monkeypatch.setenv("POSTGRES_URI", "postgresql://domain@pooler/db")
        cfg = _make_alembic_config(db_url=None)
        assert (
            cfg.get_main_option("sqlalchemy.url")
            == "postgresql+asyncpg://migration@direct/db"
        )

    def test_migration_database_uri_file(self, monkeypatch, tmp_path):
        secret_file = tmp_path / "migration-uri"
        secret_file.write_text("postgresql://migration@direct/db\n", encoding="utf-8")
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.setenv("MIGRATION_DATABASE_URI_FILE", str(secret_file))
        assert (
            _get_migration_database_uri() == "postgresql+asyncpg://migration@direct/db"
        )

    def test_migration_database_uri_sources_are_exclusive(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_DATABASE_URI", "postgresql://migration@db/app")
        monkeypatch.setenv("MIGRATION_DATABASE_URI_FILE", "/not/read")
        with pytest.raises(ValueError, match="mutually exclusive"):
            _get_migration_database_uri()

    def test_empty_migration_database_uri_fails_closed(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_DATABASE_URI", "")
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        with pytest.raises(ValueError, match="empty value"):
            _get_migration_database_uri()

    def test_falls_back_to_env(self, monkeypatch):
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        monkeypatch.delenv("POSTGRES_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("DATABASE_URI", "postgresql+asyncpg://env@host/db")
        cfg = _make_alembic_config(db_url=None)
        url = cfg.get_main_option("sqlalchemy.url")
        assert url is not None
        assert "env@host" in url

    def test_no_url_leaves_option_unset(self, monkeypatch):
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        monkeypatch.delenv("DATABASE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("POSTGRES_URI", raising=False)
        cfg = _make_alembic_config(db_url=None)
        assert cfg.get_main_option("sqlalchemy.url") is None


# ── CLI registration ───────────────────────────────────────────────


class TestRegisterCommands:
    @pytest.fixture()
    def parser(self):
        p = argparse.ArgumentParser()
        sub = p.add_subparsers(dest="command")
        register_commands(sub)
        return p

    @pytest.mark.parametrize(
        "argv",
        [
            ["migrate", "upgrade"],
            ["migrate", "upgrade", "head"],
            ["migrate", "downgrade", "-1"],
            ["migrate", "current"],
            ["migrate", "current", "-v"],
            ["migrate", "history"],
            ["migrate", "history", "--verbose"],
            ["migrate", "heads"],
        ],
    )
    def test_parses_valid_args(self, parser, argv):
        ns = parser.parse_args(argv)
        assert callable(ns.func)

    def test_upgrade_defaults_to_head(self, parser):
        ns = parser.parse_args(["migrate", "upgrade"])
        assert ns.revision == "head"

    def test_downgrade_requires_revision(self, parser):
        with pytest.raises(SystemExit):
            parser.parse_args(["migrate", "downgrade"])


# ── command dispatch (mocked alembic) ──────────────────────────────


class TestCommandDispatch:
    """Verify each _run_* function calls the correct alembic.command."""

    @patch("dev_health_ops.migrate._run_river_upgrade", return_value=0)
    @patch("alembic.command")
    def test_run_upgrade(self, mock_cmd, mock_river):
        ns = argparse.Namespace(db="sqlite:///x.db", revision="head")
        assert _run_upgrade(ns) == 0
        mock_cmd.upgrade.assert_called_once()
        _cfg, rev = mock_cmd.upgrade.call_args[0]
        assert rev == "head"
        mock_river.assert_called_once_with()

    @patch("dev_health_ops.migrate._run_river_upgrade", return_value=1)
    @patch("alembic.command")
    def test_run_upgrade_fails_closed_when_river_fails(self, mock_cmd, mock_river):
        ns = argparse.Namespace(db="sqlite:///x.db", revision="head")
        assert _run_upgrade(ns) == 1
        mock_cmd.upgrade.assert_called_once()
        mock_river.assert_called_once_with()

    @patch("dev_health_ops.migrate._run_river_upgrade")
    @patch("alembic.command")
    def test_alembic_failure_never_starts_river(self, mock_cmd, mock_river):
        mock_cmd.upgrade.side_effect = RuntimeError("alembic failed")
        ns = argparse.Namespace(db="sqlite:///x.db", revision="head")
        with pytest.raises(RuntimeError, match="alembic failed"):
            _run_upgrade(ns)
        mock_river.assert_not_called()

    @patch("dev_health_ops.migrate._run_river_upgrade")
    @patch("alembic.command")
    def test_explicit_db_cannot_split_alembic_from_river(
        self, mock_cmd, mock_river, monkeypatch
    ):
        monkeypatch.setenv(
            "MIGRATION_DATABASE_URI", "postgresql://migration@other/database"
        )
        ns = argparse.Namespace(
            db="postgresql+asyncpg://explicit@primary/database", revision="head"
        )

        with pytest.raises(ValueError, match="--db cannot be combined"):
            _run_upgrade(ns)

        mock_cmd.upgrade.assert_not_called()
        mock_river.assert_not_called()

    @patch("dev_health_ops.migrate._run_river_upgrade")
    @patch("alembic.command")
    def test_explicit_db_rejects_migration_file_before_any_ddl(
        self, mock_cmd, mock_river, monkeypatch
    ):
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.setenv("MIGRATION_DATABASE_URI_FILE", "/mounted/migration-uri")
        ns = argparse.Namespace(
            db="postgresql+asyncpg://explicit@primary/database", revision="head"
        )

        with pytest.raises(ValueError, match="--db cannot be combined"):
            _run_upgrade(ns)

        mock_cmd.upgrade.assert_not_called()
        mock_river.assert_not_called()

    @patch("alembic.command")
    def test_run_downgrade(self, mock_cmd):
        ns = argparse.Namespace(db="sqlite:///x.db", revision="-1")
        assert _run_downgrade(ns) == 0
        mock_cmd.downgrade.assert_called_once()
        _cfg, rev = mock_cmd.downgrade.call_args[0]
        assert rev == "-1"

    @patch("alembic.command")
    def test_run_current(self, mock_cmd):
        ns = argparse.Namespace(db="sqlite:///x.db", verbose=True)
        assert _run_current(ns) == 0
        mock_cmd.current.assert_called_once()

    @patch("alembic.command")
    def test_run_history(self, mock_cmd):
        ns = argparse.Namespace(db="sqlite:///x.db", verbose=False)
        assert _run_history(ns) == 0
        mock_cmd.history.assert_called_once()

    @patch("alembic.command")
    def test_run_heads(self, mock_cmd):
        ns = argparse.Namespace(db="sqlite:///x.db", verbose=False)
        assert _run_heads(ns) == 0
        mock_cmd.heads.assert_called_once()

    @patch("dev_health_ops.migrate._run_river_upgrade", return_value=0)
    @patch("alembic.command")
    def test_upgrade_uses_db_from_namespace(self, mock_cmd, _mock_river):
        ns = argparse.Namespace(
            db="postgresql+asyncpg://custom@host/mydb", revision="abc123"
        )
        _run_upgrade(ns)
        cfg = mock_cmd.upgrade.call_args[0][0]
        assert (
            cfg.get_main_option("sqlalchemy.url")
            == "postgresql+asyncpg://custom@host/mydb"
        )

    @patch("dev_health_ops.migrate._run_river_upgrade", return_value=0)
    @patch("alembic.command")
    def test_upgrade_without_db_falls_back_to_env(
        self, mock_cmd, _mock_river, monkeypatch
    ):
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        monkeypatch.delenv("POSTGRES_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("DATABASE_URI", "postgresql+asyncpg://env@host/db")
        ns = argparse.Namespace(db=None, revision="head")
        _run_upgrade(ns)
        cfg = mock_cmd.upgrade.call_args[0][0]
        url = cfg.get_main_option("sqlalchemy.url")
        assert url is not None
        assert "env@host" in url


class TestRiverMigrationDispatch:
    @patch("subprocess.run")
    def test_invokes_pinned_binary_without_shell(self, mock_run, monkeypatch):
        monkeypatch.setenv("MIGRATION_DATABASE_URI", "postgresql://migration@direct/db")
        mock_run.return_value.returncode = 0
        assert _run_river_upgrade() == 0
        mock_run.assert_called_once_with(
            ["dev-health-worker-migrate"],
            check=False,
        )

    @patch("subprocess.run")
    def test_nonzero_binary_fails_closed(self, mock_run, monkeypatch):
        monkeypatch.setenv("MIGRATION_DATABASE_URI", "postgresql://migration@direct/db")
        mock_run.return_value.returncode = 1
        assert _run_river_upgrade() == 1

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_missing_binary_fails_closed(self, _mock_run, monkeypatch):
        monkeypatch.setenv("MIGRATION_DATABASE_URI", "postgresql://migration@direct/db")
        assert _run_river_upgrade() == 1

    @patch("subprocess.run")
    def test_absent_migration_dsn_skips_additively(self, mock_run, monkeypatch):
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        assert _run_river_upgrade() == 0
        mock_run.assert_not_called()

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_empty_present_migration_dsn_does_not_skip(self, _mock_run, monkeypatch):
        monkeypatch.setenv("MIGRATION_DATABASE_URI", "")
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        assert _run_river_upgrade() == 1

    @patch("subprocess.run")
    def test_migration_dsn_file_also_enforces_binary(self, mock_run, monkeypatch):
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.setenv("MIGRATION_DATABASE_URI_FILE", "/mounted/migration-uri")
        mock_run.return_value.returncode = 0
        assert _run_river_upgrade() == 0
        mock_run.assert_called_once_with(
            ["dev-health-worker-migrate"],
            check=False,
        )
