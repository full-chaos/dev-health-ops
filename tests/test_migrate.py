"""Tests for the dev-hops migrate CLI command."""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops import migrate as migrate_mod
from dev_health_ops.migrate import (
    _ALEMBIC_DIR,
    _make_alembic_config,
    _run_current,
    _run_downgrade,
    _run_heads,
    _run_history,
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
        assert _ALEMBIC_DIR == package_dir / "alembic"


# ── _make_alembic_config ───────────────────────────────────────────


class TestMakeAlembicConfig:
    def test_sets_script_location(self):
        cfg = _make_alembic_config(db_url="sqlite:///test.db")
        assert cfg.get_main_option("script_location") == str(_ALEMBIC_DIR)

    def test_explicit_db_url(self):
        url = "postgresql+asyncpg://user:pass@host/db"
        cfg = _make_alembic_config(db_url=url)
        assert cfg.get_main_option("sqlalchemy.url") == url

    def test_falls_back_to_env(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URI", "postgresql+asyncpg://env@host/db")
        cfg = _make_alembic_config(db_url=None)
        assert "env@host" in cfg.get_main_option("sqlalchemy.url")

    def test_no_url_leaves_option_unset(self, monkeypatch):
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

    @patch("dev_health_ops.migrate.command")
    def test_run_upgrade(self, mock_cmd):
        ns = SimpleNamespace(db="sqlite:///x.db", revision="head")
        assert _run_upgrade(ns) == 0
        mock_cmd.upgrade.assert_called_once()
        _cfg, rev = mock_cmd.upgrade.call_args[0]
        assert rev == "head"

    @patch("dev_health_ops.migrate.command")
    def test_run_downgrade(self, mock_cmd):
        ns = SimpleNamespace(db="sqlite:///x.db", revision="-1")
        assert _run_downgrade(ns) == 0
        mock_cmd.downgrade.assert_called_once()
        _cfg, rev = mock_cmd.downgrade.call_args[0]
        assert rev == "-1"

    @patch("dev_health_ops.migrate.command")
    def test_run_current(self, mock_cmd):
        ns = SimpleNamespace(db="sqlite:///x.db", verbose=True)
        assert _run_current(ns) == 0
        mock_cmd.current.assert_called_once()

    @patch("dev_health_ops.migrate.command")
    def test_run_history(self, mock_cmd):
        ns = SimpleNamespace(db="sqlite:///x.db", verbose=False)
        assert _run_history(ns) == 0
        mock_cmd.history.assert_called_once()

    @patch("dev_health_ops.migrate.command")
    def test_run_heads(self, mock_cmd):
        ns = SimpleNamespace(db="sqlite:///x.db", verbose=False)
        assert _run_heads(ns) == 0
        mock_cmd.heads.assert_called_once()

    @patch("dev_health_ops.migrate.command")
    def test_upgrade_uses_db_from_namespace(self, mock_cmd):
        ns = SimpleNamespace(db="postgresql+asyncpg://custom@host/mydb", revision="abc123")
        _run_upgrade(ns)
        cfg = mock_cmd.upgrade.call_args[0][0]
        assert cfg.get_main_option("sqlalchemy.url") == "postgresql+asyncpg://custom@host/mydb"

    @patch("dev_health_ops.migrate.command")
    def test_upgrade_without_db_falls_back_to_env(self, mock_cmd, monkeypatch):
        monkeypatch.setenv("DATABASE_URI", "postgresql+asyncpg://env@host/db")
        ns = SimpleNamespace(db=None, revision="head")
        _run_upgrade(ns)
        cfg = mock_cmd.upgrade.call_args[0][0]
        assert "env@host" in cfg.get_main_option("sqlalchemy.url")
