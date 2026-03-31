from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.cli import build_parser
from dev_health_ops.migrate import (
    _run_clickhouse_status,
    _run_clickhouse_upgrade,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ns(**kwargs) -> argparse.Namespace:
    defaults = dict(analytics_db="clickhouse://fake:8123/test", db=None)
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# CLI routing — every path resolves to the expected function
# ---------------------------------------------------------------------------


class TestMigrateRouting:
    @pytest.fixture(autouse=True)
    def _parser(self):
        self.parser = build_parser()

    @pytest.mark.parametrize(
        "argv, expected_func",
        [
            (["migrate", "postgres"], "_run_upgrade"),
            (["migrate", "postgres", "upgrade"], "_run_upgrade"),
            (["migrate", "postgres", "upgrade", "abc123"], "_run_upgrade"),
            (["migrate", "postgres", "downgrade", "base"], "_run_downgrade"),
            (["migrate", "postgres", "current"], "_run_current"),
            (["migrate", "postgres", "history"], "_run_history"),
            (["migrate", "postgres", "heads"], "_run_heads"),
            (["migrate", "clickhouse"], "_run_clickhouse_upgrade"),
            (["migrate", "clickhouse", "upgrade"], "_run_clickhouse_upgrade"),
            (["migrate", "clickhouse", "status"], "_run_clickhouse_status"),
            # backward-compat flat aliases
            (["migrate", "upgrade"], "_run_upgrade"),
            (["migrate", "upgrade", "abc123"], "_run_upgrade"),
            (["migrate", "downgrade", "base"], "_run_downgrade"),
            (["migrate", "current"], "_run_current"),
            (["migrate", "history"], "_run_history"),
            (["migrate", "heads"], "_run_heads"),
        ],
    )
    def test_routing(self, argv: list[str], expected_func: str):
        ns = self.parser.parse_args(argv)
        assert ns.func.__name__ == expected_func

    def test_postgres_bare_defaults_to_upgrade_head(self):
        ns = self.parser.parse_args(["migrate", "postgres"])
        assert ns.func.__name__ == "_run_upgrade"
        assert ns.revision == "head"

    def test_clickhouse_bare_defaults_to_upgrade(self):
        ns = self.parser.parse_args(["migrate", "clickhouse"])
        assert ns.func.__name__ == "_run_clickhouse_upgrade"

    def test_upgrade_flat_defaults_to_head(self):
        ns = self.parser.parse_args(["migrate", "upgrade"])
        assert ns.revision == "head"

    def test_upgrade_flat_accepts_revision(self):
        ns = self.parser.parse_args(["migrate", "upgrade", "abc123"])
        assert ns.revision == "abc123"


# ---------------------------------------------------------------------------
# _run_clickhouse_upgrade
# ---------------------------------------------------------------------------


class TestClickhouseUpgrade:
    def test_calls_ensure_schema_and_close(self):
        mock_sink = MagicMock()
        mock_sink_cls = MagicMock(return_value=mock_sink)

        with (
            patch(
                "dev_health_ops.migrate.resolve_sink_uri",
                return_value="clickhouse://fake:8123/test",
            ),
            patch(
                "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
                mock_sink_cls,
            ),
        ):
            result = _run_clickhouse_upgrade(_ns())

        assert result == 0
        mock_sink_cls.assert_called_once_with(dsn="clickhouse://fake:8123/test")
        mock_sink.ensure_schema.assert_called_once()
        mock_sink.close.assert_called_once()

    def test_close_called_even_on_failure(self):
        mock_sink = MagicMock()
        mock_sink.ensure_schema.side_effect = RuntimeError("boom")
        mock_sink_cls = MagicMock(return_value=mock_sink)

        with (
            patch(
                "dev_health_ops.migrate.resolve_sink_uri",
                return_value="clickhouse://fake:8123/test",
            ),
            patch(
                "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
                mock_sink_cls,
            ),
            pytest.raises(RuntimeError, match="boom"),
        ):
            _run_clickhouse_upgrade(_ns())

        mock_sink.close.assert_called_once()


# ---------------------------------------------------------------------------
# _run_clickhouse_status
# ---------------------------------------------------------------------------


class TestClickhouseStatus:
    def _make_mock_client(self, applied_rows: list | None = None):
        mock_client = MagicMock()
        result = MagicMock()
        result.result_rows = applied_rows or []
        mock_client.query.return_value = result
        return mock_client

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_shows_applied_and_pending(self, _mock_uri, tmp_path, capsys):
        (tmp_path / "000_init.sql").write_text("CREATE TABLE t (x Int32) ENGINE=Memory")
        (tmp_path / "001_add_col.sql").write_text("ALTER TABLE t ADD COLUMN y String")

        applied_rows = [("000_init.sql", datetime(2025, 1, 15, 10, 30))]
        mock_client = self._make_mock_client(applied_rows)

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        out = capsys.readouterr().out
        assert "[applied" in out
        assert "000_init.sql" in out
        assert "[pending]" in out
        assert "001_add_col.sql" in out
        assert "1 applied, 1 pending, 2 total" in out
        mock_client.close.assert_called_once()

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_all_pending_when_no_schema_migrations_table(
        self, _mock_uri, tmp_path, capsys
    ):
        (tmp_path / "000_init.sql").write_text("SELECT 1")

        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Unknown table")

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        out = capsys.readouterr().out
        assert "[pending]" in out
        assert "0 applied, 1 pending, 1 total" in out

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_no_migration_files(self, _mock_uri, tmp_path, capsys):
        mock_client = self._make_mock_client()
        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        assert "No ClickHouse migration files found." in capsys.readouterr().out

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_includes_py_migration_files(self, _mock_uri, tmp_path, capsys):
        (tmp_path / "000_init.sql").write_text("SELECT 1")
        (tmp_path / "001_migrate.py").write_text("def upgrade(client): pass")

        mock_client = self._make_mock_client()

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        out = capsys.readouterr().out
        assert "000_init.sql" in out
        assert "001_migrate.py" in out
        assert "0 applied, 2 pending, 2 total" in out

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_all_applied(self, _mock_uri, tmp_path, capsys):
        (tmp_path / "000_init.sql").write_text("SELECT 1")

        applied_rows = [("000_init.sql", datetime(2025, 6, 1))]
        mock_client = self._make_mock_client(applied_rows)

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        out = capsys.readouterr().out
        assert "1 applied, 0 pending, 1 total" in out
        assert "[pending]" not in out

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_close_called_even_on_failure(self, _mock_uri, tmp_path):
        (tmp_path / "000_init.sql").write_text("SELECT 1")

        mock_client = self._make_mock_client()
        mock_client.query.return_value.result_rows = []

        exploding_path = MagicMock(spec=Path)
        exploding_path.glob.side_effect = OSError("disk error")
        exploding_path.exists.return_value = True

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", exploding_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            with pytest.raises(OSError, match="disk error"):
                _run_clickhouse_status(_ns())

        mock_client.close.assert_called_once()

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_all_pending_when_no_schema_migrations_table(
        self, _mock_uri, tmp_path, capsys
    ):
        (tmp_path / "000_init.sql").write_text("SELECT 1")

        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Unknown table")

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        out = capsys.readouterr().out
        assert "[pending]" in out
        assert "0 applied, 1 pending, 1 total" in out

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_no_migration_files(self, _mock_uri, tmp_path, capsys):
        mock_client = self._make_mock_client()
        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        assert "No ClickHouse migration files found." in capsys.readouterr().out

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_includes_py_migration_files(self, _mock_uri, tmp_path, capsys):
        (tmp_path / "000_init.sql").write_text("SELECT 1")
        (tmp_path / "001_migrate.py").write_text("def upgrade(client): pass")

        mock_client = self._make_mock_client()

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        out = capsys.readouterr().out
        assert "000_init.sql" in out
        assert "001_migrate.py" in out
        assert "0 applied, 2 pending, 2 total" in out

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_all_applied(self, _mock_uri, tmp_path, capsys):
        (tmp_path / "000_init.sql").write_text("SELECT 1")

        applied_rows = [("000_init.sql", datetime(2025, 6, 1))]
        mock_client = self._make_mock_client(applied_rows)

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", tmp_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            result = _run_clickhouse_status(_ns())

        assert result == 0
        out = capsys.readouterr().out
        assert "1 applied, 0 pending, 1 total" in out
        assert "[pending]" not in out

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_close_called_even_on_failure(self, _mock_uri, tmp_path):
        (tmp_path / "000_init.sql").write_text("SELECT 1")

        mock_client = self._make_mock_client()
        mock_client.query.return_value.result_rows = []

        exploding_path = MagicMock(spec=Path)
        exploding_path.glob.side_effect = OSError("disk error")
        exploding_path.exists.return_value = True

        with (
            patch("dev_health_ops.migrate._CH_MIGRATIONS_DIR", exploding_path),
            patch("clickhouse_connect.get_client", return_value=mock_client),
        ):
            with pytest.raises(OSError, match="disk error"):
                _run_clickhouse_status(_ns())

        mock_client.close.assert_called_once()
