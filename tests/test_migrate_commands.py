from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.cli import build_parser
from dev_health_ops.migrate import (
    _run_clickhouse_repair,
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
            (["migrate", "clickhouse", "repair"], "_run_clickhouse_repair"),
            (["migrate", "clickhouse", "repair", "--apply"], "_run_clickhouse_repair"),
            (
                ["migrate", "clickhouse", "repair", "--apply", "--org", "org-a"],
                "_run_clickhouse_repair",
            ),
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


# ---------------------------------------------------------------------------
# _run_clickhouse_repair  (CHAOS-1776)
# ---------------------------------------------------------------------------


class TestClickhouseRepair:
    def _make_client(self, orphan_rows: list | None = None) -> MagicMock:
        mock_client = MagicMock()
        result = MagicMock()
        result.result_rows = orphan_rows or []
        mock_client.query.return_value = result
        return mock_client

    def _ns_repair(self, **kwargs):
        kwargs.setdefault("apply", False)
        kwargs.setdefault("org", None)
        return _ns(**kwargs)

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_no_orphans_prints_clean_message(self, _uri, capsys):
        mock_client = self._make_client(orphan_rows=[])

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            result = _run_clickhouse_repair(self._ns_repair())

        assert result == 0
        assert "No stale-tenant orphans found in repos." in capsys.readouterr().out
        # Detection-only path — no ALTER TABLE DELETE.
        mock_client.command.assert_not_called()
        mock_client.close.assert_called_once()

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_dry_run_does_not_delete(self, _uri, capsys):
        """Default invocation prints orphans but issues no ALTER TABLE DELETE."""
        orphan_rows = [
            (
                "e50b01bd-47a3-50e7-a9b4-d29a95bfdb07",
                "acme/demo-app-1",
                "stale-org",
                "active-org",
                datetime(2026, 5, 1),
                datetime(2026, 5, 22),
            ),
        ]
        mock_client = self._make_client(orphan_rows=orphan_rows)

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            result = _run_clickhouse_repair(self._ns_repair())

        assert result == 0
        out = capsys.readouterr().out
        assert "Found 1 stale-tenant orphan row(s) in repos" in out
        assert "acme/demo-app-1" in out
        assert "stale-org" in out
        assert "active-org" in out
        assert "Dry-run: pass --apply to delete these orphan rows." in out
        # No DELETE issued in dry-run.
        mock_client.command.assert_not_called()
        mock_client.close.assert_called_once()

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_apply_issues_delete_per_orphan(self, _uri, capsys):
        orphan_rows = [
            (
                "e50b01bd-47a3-50e7-a9b4-d29a95bfdb07",
                "acme/demo-app-1",
                "stale-org-a",
                "active-org",
                datetime(2026, 5, 1),
                datetime(2026, 5, 22),
            ),
            (
                "5b862ba7-ef97-59d6-aa06-d1b30de9112b",
                "acme/demo-app-4",
                "stale-org-b",
                "active-org",
                datetime(2026, 4, 1),
                datetime(2026, 5, 22),
            ),
        ]
        mock_client = self._make_client(orphan_rows=orphan_rows)

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            result = _run_clickhouse_repair(self._ns_repair(apply=True))

        assert result == 0
        assert mock_client.command.call_count == 2

        for call, expected in zip(
            mock_client.command.call_args_list,
            [
                ("e50b01bd-47a3-50e7-a9b4-d29a95bfdb07", "stale-org-a"),
                ("5b862ba7-ef97-59d6-aa06-d1b30de9112b", "stale-org-b"),
            ],
            strict=True,
        ):
            args, kwargs = call
            sql = args[0]
            assert "ALTER TABLE repos DELETE" in sql
            assert "mutations_sync=2" in sql
            assert kwargs["parameters"] == {"id": expected[0], "org": expected[1]}

        out = capsys.readouterr().out
        assert "Deleted 2 stale-tenant row(s) from repos." in out
        mock_client.close.assert_called_once()

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_org_filter_passes_param_to_detect_query(self, _uri):
        mock_client = self._make_client(orphan_rows=[])

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            _run_clickhouse_repair(self._ns_repair(org="my-org-uuid"))

        assert mock_client.query.call_count == 1
        args, kwargs = mock_client.query.call_args
        detect_sql = args[0]
        # Org filter clause must be injected when --org is given.
        assert "AND l.active_org_id = {active_org:String}" in detect_sql
        assert kwargs["parameters"] == {"active_org": "my-org-uuid"}

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_no_org_filter_omits_clause(self, _uri):
        mock_client = self._make_client(orphan_rows=[])

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            _run_clickhouse_repair(self._ns_repair())

        args, kwargs = mock_client.query.call_args
        detect_sql = args[0]
        # Without --org, the org filter clause must NOT appear.
        assert "l.active_org_id =" not in detect_sql
        assert kwargs["parameters"] == {}

    @patch(
        "dev_health_ops.migrate.resolve_sink_uri",
        return_value="clickhouse://fake:8123/test",
    )
    def test_close_called_even_on_failure(self, _uri):
        mock_client = self._make_client()
        mock_client.query.side_effect = RuntimeError("boom")

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="boom"):
                _run_clickhouse_repair(self._ns_repair())

        mock_client.close.assert_called_once()
