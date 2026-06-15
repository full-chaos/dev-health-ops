"""Tests for CHAOS-2440: quiet + fast-path migration ensure.

Covers both runner paths:
- ClickHouseCore._apply_sql_migrations  (metrics sink, sync)
- ClickHouseStore._ensure_tables        (storage, async)

Fast-path contract
------------------
The per-file loop is skipped entirely ONLY when EVERY on-disk migration is
already recorded as applied (full-set completeness check —
``all_migrations_applied``).  Proof: no INSERT INTO schema_migrations is issued
and query() is called only once (the initial SELECT).

Critically, the fast-path must NOT fire when an *intermediate* migration is
missing even if the lexicographically-latest one is applied (this repo has
inserted / mixed-ordering migrations like '023b_' and duplicate numeric
prefixes — a latest-only check would silently skip the gap, CHAOS-2440).

Quiet contract
--------------
Per-migration "Skipping already applied migration: X" lines must be emitted at
DEBUG level, never INFO, so normal CLI output stays clean.

Ordering contract
-----------------
Pending migrations apply in the same ``sorted(*.sql + *.py)`` order, so exotic
names like '023b_dora_metrics.sql' sort correctly between '023_' and '024_'.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.sinks.clickhouse.core import ClickHouseCore
from dev_health_ops.migrations.clickhouse import all_migrations_applied
from dev_health_ops.storage import ClickHouseStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _applied_result(versions: list[str]) -> MagicMock:
    result = MagicMock()
    result.result_rows = [(v,) for v in versions]
    return result


def _make_core_sink(client: MagicMock) -> ClickHouseMetricsSink:
    """Return a concrete ClickHouseMetricsSink (which inherits ClickHouseCore) with a fake client."""
    return ClickHouseMetricsSink(
        dsn="clickhouse://ch:ch@localhost:8123/default", client=client
    )


def _make_store(client: MagicMock) -> ClickHouseStore:
    """Build a ClickHouseStore without opening a real connection."""
    store = ClickHouseStore.__new__(ClickHouseStore)
    store._lock = asyncio.Lock()
    store._settings = {}
    store.org_id = None
    store.client = client
    return store


def _real_migration_files_for(
    method_code_filename: str, parents_depth: int
) -> list[Path]:
    """Return the sorted migration file list the runner would compute at runtime."""
    migrations_dir = (
        Path(method_code_filename).resolve().parents[parents_depth]
        / "migrations"
        / "clickhouse"
    )
    return sorted(
        list(migrations_dir.glob("*.sql")) + list(migrations_dir.glob("*.py"))
    )


async def _fake_to_thread(fn, *args, **kwargs):
    """Collapse asyncio.to_thread into a direct synchronous call for testing."""
    return fn(*args, **kwargs)


# ---------------------------------------------------------------------------
# all_migrations_applied — shared full-set completeness helper
# ---------------------------------------------------------------------------


class TestAllMigrationsApplied:
    def test_empty_disk_is_trivially_applied(self) -> None:
        assert all_migrations_applied([], ["000_x.sql"]) is True

    def test_all_present_returns_true(self) -> None:
        files = ["000_a.sql", "001_b.sql", "002_c.sql"]
        assert all_migrations_applied(files, files) is True

    def test_extra_applied_rows_are_ignored(self) -> None:
        # DB may have rows for files no longer on disk; completeness only cares
        # that every ON-DISK file is applied.
        files = ["000_a.sql", "001_b.sql"]
        applied = ["000_a.sql", "001_b.sql", "999_removed.sql"]
        assert all_migrations_applied(files, applied) is True

    def test_missing_latest_returns_false(self) -> None:
        files = ["000_a.sql", "001_b.sql", "002_c.sql"]
        applied = ["000_a.sql", "001_b.sql"]
        assert all_migrations_applied(files, applied) is False

    def test_missing_middle_with_latest_present_returns_false(self) -> None:
        """The core regression: latest applied but a gap in the middle."""
        files = ["000_a.sql", "001_b.sql", "002_c.sql"]
        applied = ["000_a.sql", "002_c.sql"]  # 001 missing, latest 002 present
        assert all_migrations_applied(files, applied) is False


# ---------------------------------------------------------------------------
# ClickHouseCore._apply_sql_migrations
# ---------------------------------------------------------------------------


class TestApplySqlMigrationsCoreFastPath:
    def _real_files(self) -> list[Path]:
        # _apply_sql_migrations lives in core.py; use ClickHouseCore's __code__
        # to get the source file so parents[3] resolves correctly.
        return _real_migration_files_for(
            ClickHouseCore._apply_sql_migrations.__code__.co_filename,
            parents_depth=3,
        )

    def _client_with_all_applied(self, files: list[Path]) -> MagicMock:
        client = MagicMock()
        client.query.return_value = _applied_result([p.name for p in files])
        return client

    # ------------------------------------------------------------------
    # Fast-path: up-to-date DB
    # ------------------------------------------------------------------

    def test_up_to_date_issues_no_migration_inserts(self) -> None:
        """When every file on disk is applied, no INSERT INTO schema_migrations fires."""
        files = self._real_files()
        assert files, "No real migration files found"

        commands: list[str] = []
        client = self._client_with_all_applied(files)
        client.command.side_effect = lambda sql, parameters=None: commands.append(sql)

        _make_core_sink(client)._apply_sql_migrations()

        inserts = [c for c in commands if "INSERT INTO schema_migrations" in c]
        assert inserts == [], (
            f"Fast-path missed: INSERT issued when DB is current. Commands: {inserts}"
        )

    def test_up_to_date_queries_db_exactly_once(self) -> None:
        """Fast-path must not issue multiple SELECTs against schema_migrations."""
        files = self._real_files()
        client = self._client_with_all_applied(files)
        _make_core_sink(client)._apply_sql_migrations()
        # One CREATE TABLE + one SELECT → query count must be 1.
        assert client.query.call_count == 1

    # ------------------------------------------------------------------
    # Quiet: skip logs must be DEBUG not INFO
    # ------------------------------------------------------------------

    def test_skip_logs_are_debug_not_info(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """'Skipping already applied migration' must never appear at INFO level."""
        files = self._real_files()
        client = self._client_with_all_applied(files)

        with caplog.at_level(logging.DEBUG, logger="dev_health_ops"):
            _make_core_sink(client)._apply_sql_migrations()

        info_skips = [
            r
            for r in caplog.records
            if r.levelno >= logging.INFO
            and "Skipping already applied migration" in r.message
        ]
        assert info_skips == [], (
            f"Found INFO-level skip log(s): {[r.message for r in info_skips]}"
        )

    # ------------------------------------------------------------------
    # Correctness: pending migrations still applied
    # ------------------------------------------------------------------

    def test_pending_migration_is_applied_and_recorded(self) -> None:
        """When DB is one migration behind, that migration is recorded."""
        files = self._real_files()
        assert len(files) >= 2, "Need at least 2 migrations for this test"

        # All-but-last applied.
        applied = [p.name for p in files[:-1]]
        pending = files[-1]

        client = MagicMock()
        client.query.return_value = _applied_result(applied)
        commands: list[str] = []
        client.command.side_effect = lambda sql, parameters=None: commands.append(sql)

        _make_core_sink(client)._apply_sql_migrations()

        inserts = [c for c in commands if "INSERT INTO schema_migrations" in c]
        assert len(inserts) == 1, (
            f"Expected 1 schema_migrations INSERT for {pending.name!r}, got {inserts}"
        )

    # ------------------------------------------------------------------
    # Correctness (the critical case): latest applied but a MIDDLE migration
    # is missing → fast-path must NOT fire and the gap must be filled.
    # ------------------------------------------------------------------

    def test_missing_middle_migration_is_applied_despite_latest_present(self) -> None:
        """Latest IS applied but an intermediate migration is ABSENT.

        A latest-filename-only fast-path would falsely short-circuit here and
        silently skip the missing middle migration (schema drift). The full-set
        check must detect the gap, run the loop, and apply exactly the missing
        migration (recording only it).
        """
        files = self._real_files()
        assert len(files) >= 3, "Need at least 3 migrations to drop a middle one"

        # Drop a migration from the MIDDLE; keep the latest applied.
        missing_idx = len(files) // 2
        missing = files[missing_idx]
        applied = [p.name for i, p in enumerate(files) if i != missing_idx]
        assert files[-1].name in applied, "Latest must remain applied for this test"

        client = MagicMock()
        client.query.return_value = _applied_result(applied)
        recorded_versions: list[str] = []

        def record_command(sql: str, parameters: dict | None = None) -> None:
            if "INSERT INTO schema_migrations" in sql and parameters:
                recorded_versions.append(parameters["version"])

        client.command.side_effect = record_command

        _make_core_sink(client)._apply_sql_migrations()

        assert recorded_versions == [missing.name], (
            f"Expected exactly the missing middle migration {missing.name!r} to be "
            f"applied; got {recorded_versions}"
        )

    # ------------------------------------------------------------------
    # Ordering: mixed .sql/.py names with b-suffix sort correctly
    # ------------------------------------------------------------------

    def test_mixed_sql_py_ordering_matches_fast_path(self) -> None:
        """Pending migrations apply in the same sort order as the file list.

        Exercises the '023b_' naming convention: it must sort between '023_'
        and '024_'. With all files applied the full-set fast-path fires.
        """
        files = self._real_files()
        names = [p.name for p in files]

        # Verify corpus has mixed types.
        exts = {p.suffix for p in files}
        assert ".sql" in exts and ".py" in exts

        # '023b_dora_metrics.sql' must appear between '023_*' and '024_*'.
        b_files = [n for n in names if n.startswith("023b")]
        assert b_files, "Expected at least one '023b_*' migration in the corpus"
        idx_b = names.index(b_files[0])
        after_b = names[idx_b + 1 :]
        assert any(n.startswith("024") for n in after_b), (
            "'024_*' migration must appear after '023b_*' in sorted order"
        )

        # Applying all files must hit the fast-path (latest == files[-1].name).
        client = self._client_with_all_applied(files)
        commands: list[str] = []
        client.command.side_effect = lambda sql, parameters=None: commands.append(sql)

        _make_core_sink(client)._apply_sql_migrations()

        inserts = [c for c in commands if "INSERT INTO schema_migrations" in c]
        assert inserts == [], (
            f"Fast-path did not trigger for latest={files[-1].name!r}: {inserts}"
        )


# ---------------------------------------------------------------------------
# ClickHouseStore._ensure_tables
# ---------------------------------------------------------------------------


class TestEnsureTablesFastPath:
    def _real_files(self) -> list[Path]:
        return _real_migration_files_for(
            ClickHouseStore._ensure_tables.__code__.co_filename,
            parents_depth=1,
        )

    @pytest.mark.asyncio
    async def test_up_to_date_issues_no_migration_inserts(self) -> None:
        """Fast-path: no INSERT INTO schema_migrations when DB is current."""
        files = self._real_files()
        assert files

        client = MagicMock()
        client.query.return_value = _applied_result([p.name for p in files])
        commands: list[str] = []
        client.command.side_effect = lambda sql, parameters=None: commands.append(sql)

        store = _make_store(client)
        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            await store._ensure_tables()

        inserts = [c for c in commands if "INSERT INTO schema_migrations" in c]
        assert inserts == [], f"Fast-path missed: {inserts}"

    @pytest.mark.asyncio
    async def test_up_to_date_queries_db_exactly_once(self) -> None:
        """Only one query call (the version SELECT) when schema is current."""
        files = self._real_files()

        client = MagicMock()
        client.query.return_value = _applied_result([p.name for p in files])

        store = _make_store(client)
        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            await store._ensure_tables()

        assert client.query.call_count == 1

    @pytest.mark.asyncio
    async def test_skip_logs_are_debug_not_info(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """'Skipping already applied migration' must not appear at INFO level."""
        files = self._real_files()

        client = MagicMock()
        client.query.return_value = _applied_result([p.name for p in files])

        store = _make_store(client)
        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            with caplog.at_level(logging.DEBUG, logger="dev_health_ops"):
                await store._ensure_tables()

        info_skips = [
            r
            for r in caplog.records
            if r.levelno >= logging.INFO
            and "Skipping already applied migration" in r.message
        ]
        assert info_skips == [], f"INFO skip log(s): {[r.message for r in info_skips]}"

    @pytest.mark.asyncio
    async def test_pending_migration_is_applied_and_recorded(self) -> None:
        """When DB is one migration behind, that migration is recorded."""
        files = self._real_files()
        assert len(files) >= 2

        applied = [p.name for p in files[:-1]]
        pending = files[-1]

        client = MagicMock()
        client.query.return_value = _applied_result(applied)
        commands: list[str] = []
        client.command.side_effect = lambda sql, parameters=None: commands.append(sql)

        store = _make_store(client)
        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            await store._ensure_tables()

        inserts = [c for c in commands if "INSERT INTO schema_migrations" in c]
        assert len(inserts) == 1, (
            f"Expected 1 INSERT for {pending.name!r}, got {inserts}"
        )

    @pytest.mark.asyncio
    async def test_missing_middle_migration_is_applied_despite_latest_present(
        self,
    ) -> None:
        """Latest IS applied but an intermediate migration is ABSENT (async path).

        The full-set fast-path must detect the gap and apply exactly the missing
        middle migration — a latest-only check would silently skip it.
        """
        files = self._real_files()
        assert len(files) >= 3, "Need at least 3 migrations to drop a middle one"

        missing_idx = len(files) // 2
        missing = files[missing_idx]
        applied = [p.name for i, p in enumerate(files) if i != missing_idx]
        assert files[-1].name in applied, "Latest must remain applied for this test"

        client = MagicMock()
        client.query.return_value = _applied_result(applied)
        recorded_versions: list[str] = []

        def record_command(sql: str, parameters: dict | None = None) -> None:
            if "INSERT INTO schema_migrations" in sql and parameters:
                recorded_versions.append(parameters["version"])

        client.command.side_effect = record_command

        store = _make_store(client)
        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            await store._ensure_tables()

        assert recorded_versions == [missing.name], (
            f"Expected exactly the missing middle migration {missing.name!r} to be "
            f"applied; got {recorded_versions}"
        )
