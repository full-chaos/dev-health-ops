"""Migration 075 (CHAOS-3563 round-3 review A): reconcile
`project_declared_state_history` to the current schema.

074 was amended in place multiple times during development (see 074's own
docstring); a database that already recorded "074" as applied, at ANY
earlier shape, never re-runs it. 075 detects a stale shape (missing
`version_key`, or a missing floor table) and rebuilds both tables from
`projects FINAL`; it is a no-op when the shape is already current.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
)
MIGRATION_075 = "075_reconcile_project_declared_state_history.py"


def _load() -> ModuleType:
    path = MIGRATIONS_DIR / MIGRATION_075
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Result:
    def __init__(self, rows: list[list[Any]]) -> None:
        self.result_rows = rows


class _FakeClient:
    """Simulates a database whose `project_declared_state_history` /
    `project_declared_state_floor` tables are in a controllable shape.
    """

    def __init__(
        self,
        *,
        history_exists: bool = True,
        has_version_key: bool = True,
        floor_exists: bool = True,
        projects_exists: bool = True,
    ) -> None:
        self._history_exists = history_exists
        self._has_version_key = has_version_key
        self._floor_exists = floor_exists
        self._projects_exists = projects_exists
        self.commands: list[str] = []

    def query(self, q: str, parameters: dict | None = None) -> _Result:
        if "FROM system.tables" in q:
            table = str((parameters or {}).get("name") or "")
            exists = {
                "project_declared_state_history": self._history_exists,
                "project_declared_state_floor": self._floor_exists,
                "projects": self._projects_exists,
            }.get(table, False)
            return _Result([[1 if exists else 0]])
        if "FROM system.columns" in q:
            column = str((parameters or {}).get("column") or "")
            table = str((parameters or {}).get("table") or "")
            found = (
                table == "project_declared_state_history"
                and column == "version_key"
                and self._has_version_key
            )
            return _Result([[1 if found else 0]])
        return _Result([])

    def command(self, cmd: str, parameters: dict | None = None) -> None:
        self.commands.append(cmd)


def test_noop_when_shape_is_already_current() -> None:
    """The common case: a fresh DB, or a DB that only ever ran the CURRENT
    074 -- 075 must issue NO commands at all.
    """
    module = _load()
    client = _FakeClient(history_exists=True, has_version_key=True, floor_exists=True)
    module.upgrade(client)
    assert client.commands == [], (
        "an already-current shape must produce zero DROP/CREATE/INSERT "
        "commands -- this migration must be a true no-op in the common case"
    )


def test_reconciles_when_version_key_is_missing() -> None:
    """The exact defect this migration exists to heal: a DB that ran an
    OLDER shape of 074 (no `version_key` column at all) -- 075 must DROP,
    recreate with the CURRENT shape, and re-seed both tables.
    """
    module = _load()
    client = _FakeClient(history_exists=True, has_version_key=False, floor_exists=True)
    module.upgrade(client)

    normalized_commands = [" ".join(c.split()) for c in client.commands]
    assert any(
        "DROP TABLE IF EXISTS project_declared_state_history" in c
        for c in normalized_commands
    )
    assert any(
        "DROP TABLE IF EXISTS project_declared_state_floor" in c
        for c in normalized_commands
    )
    create_history = next(
        c
        for c in normalized_commands
        if "CREATE TABLE" in c and "project_declared_state_history" in c
    )
    assert "version_key UInt64 MATERIALIZED" in create_history
    assert "cityHash64(" in create_history
    assert any(
        "INSERT INTO project_declared_state_history" in c for c in normalized_commands
    )
    assert any(
        "INSERT INTO project_declared_state_floor" in c for c in normalized_commands
    )
    # DROP must happen BEFORE CREATE for each table -- rebuilding into an
    # existing stale table would be a syntax/semantics error.
    drop_idx = next(
        i
        for i, c in enumerate(normalized_commands)
        if "DROP TABLE IF EXISTS project_declared_state_history" in c
    )
    create_idx = next(
        i
        for i, c in enumerate(normalized_commands)
        if "CREATE TABLE" in c and "project_declared_state_history" in c
    )
    assert drop_idx < create_idx


def test_reconciles_when_floor_table_is_missing() -> None:
    """The other stale-shape trigger: `version_key` present (a DB that ran
    the write_seq-masked-low-bits shape, which DID have version_key) but
    no floor table at all (the is_backfill_floor-column era, or an even
    older shape) -- must still trigger a rebuild.
    """
    module = _load()
    client = _FakeClient(history_exists=True, has_version_key=True, floor_exists=False)
    module.upgrade(client)
    assert any(
        "DROP TABLE IF EXISTS project_declared_state_floor" in " ".join(c.split())
        for c in client.commands
    )


def test_reconciles_a_completely_fresh_db_that_lacks_the_history_table() -> None:
    """No history table at all yet (074 never ran, e.g. this migration
    somehow runs before 074 in a test harness) -- must still heal to a
    correct shape rather than erroring.
    """
    module = _load()
    client = _FakeClient(
        history_exists=False, has_version_key=False, floor_exists=False
    )
    module.upgrade(client)
    assert any(
        "CREATE TABLE" in c and "project_declared_state_history" in c
        for c in client.commands
    )


def test_skips_backfill_when_projects_table_is_absent() -> None:
    module = _load()
    client = _FakeClient(
        history_exists=True,
        has_version_key=False,
        floor_exists=True,
        projects_exists=False,
    )
    module.upgrade(client)  # must not raise
    assert not any("INSERT INTO" in c for c in client.commands)


def test_upgrade_is_idempotent_against_a_second_run() -> None:
    """After the first (healing) run, the SAME fake client's tables are now
    "current" from 075's own perspective in a real DB -- but since this
    fake doesn't mutate its own state based on commands issued, we instead
    verify directly: calling upgrade() on an ALREADY-current fake a second
    time is the documented no-op path (covered above), and calling it
    twice on a STALE fake reissues the identical rebuild both times
    (deterministic, safe to repeat).
    """
    module = _load()
    client = _FakeClient(history_exists=True, has_version_key=False, floor_exists=True)
    module.upgrade(client)
    first_commands = list(client.commands)
    module.upgrade(client)
    second_commands = client.commands[len(first_commands) :]
    assert first_commands == second_commands
