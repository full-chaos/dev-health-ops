"""Migration 074 (CHAOS-3563): retain declared-state history for projects.

`projects` is a ReplacingMergeTree keyed on (org_id, provider, id) -- a merge
physically discards every earlier version sharing that key, so once a
project's declared state changes there is no history left to read (the
documented gap this migration closes -- see native_status_change.py's
`_PROJECT_DECLARED_FACTS_SQL` comment and CHAOS-3563).

`project_declared_state_history` is additive and keyed one level finer, by
(org_id, provider, id, updated_at): only a genuine re-sync of an UNCHANGED
declared state (same provider mtime) collapses via ReplacingMergeTree; a real
state change carries a new `updated_at` and is retained as a new row forever.
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
MIGRATION_074 = "074_project_declared_state_history.py"


def _load() -> ModuleType:
    path = MIGRATIONS_DIR / MIGRATION_074
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Result:
    def __init__(self, rows: list[list[Any]]) -> None:
        self.result_rows = rows


class _FakeClient:
    def __init__(self, *, projects_exists: bool = True) -> None:
        self._projects_exists = projects_exists
        self.commands: list[str] = []

    def query(self, q: str, parameters: dict | None = None) -> _Result:
        if "FROM system.tables" in q:
            return _Result([[1 if self._projects_exists else 0]])
        return _Result([])

    def command(self, cmd: str, parameters: dict | None = None) -> None:
        self.commands.append(cmd)


def test_creates_the_history_table() -> None:
    module = _load()
    client = _FakeClient()
    module.upgrade(client)
    assert any(
        "CREATE TABLE IF NOT EXISTS project_declared_state_history" in c
        for c in client.commands
    )


def test_history_table_is_keyed_finer_than_projects_by_updated_at() -> None:
    """The whole fix: `projects` collapses on (org_id, provider, id) alone --
    a real declared-state change is invisible to ReplacingMergeTree because
    it shares that key with the row it replaces. The history table's ORDER BY
    must also include `updated_at` so a genuine state change (a new provider
    mtime) is a NEW key, never collapsed away.
    """
    module = _load()
    client = _FakeClient()
    module.upgrade(client)
    create_stmt = next(c for c in client.commands if "CREATE TABLE" in c)
    normalized = " ".join(create_stmt.split())
    assert "ReplacingMergeTree" in normalized
    assert "ORDER BY (org_id, provider, id, updated_at)" in normalized


def test_backfills_from_projects_final_when_projects_exists() -> None:
    module = _load()
    client = _FakeClient(projects_exists=True)
    module.upgrade(client)
    backfill = [c for c in client.commands if "INSERT INTO" in c]
    assert backfill, "expected a backfill INSERT when projects exists"
    assert "FROM projects FINAL" in backfill[0]


def test_backfill_marks_every_seeded_row_as_the_floor() -> None:
    """PR #1602 review F4 (CONFIRMED): the backfill INSERT is the ONLY
    writer that may ever set `is_backfill_floor = 1` -- it is how a reader
    (`_PROJECT_DECLARED_FACTS_SQL`'s `earliest_is_backfill_floor`) tells a
    genuine floor breach (real state existed before this seed, now
    unrecoverable) apart from a project simply created after this
    migration ran (no seed at all, full history already known).
    """
    module = _load()
    client = _FakeClient(projects_exists=True)
    module.upgrade(client)
    backfill = next(c for c in client.commands if "INSERT INTO" in c)
    normalized = " ".join(backfill.split())
    assert "is_backfill_floor" in normalized
    # The literal `1` must be a projected value in the SELECT list (every
    # backfilled row), not merely present anywhere in the statement text.
    assert "target_date, url, updated_at, last_synced, 1" in normalized


def test_history_table_has_an_is_backfill_floor_column_defaulting_to_zero() -> None:
    module = _load()
    client = _FakeClient()
    module.upgrade(client)
    create_stmt = next(c for c in client.commands if "CREATE TABLE" in c)
    normalized = " ".join(create_stmt.split())
    assert "is_backfill_floor UInt8 DEFAULT 0" in normalized, (
        "every ordinary writer (production syncs, fixtures) omits this "
        "column and must get 0 from the schema default, never 1"
    )


def test_backfill_is_the_only_recoverable_state_documented_as_such() -> None:
    """Regression guard for the module docstring's central claim: the
    backfill source is `projects FINAL` -- the single current row per
    project -- not some richer historical source, because none exists.
    """
    module = _load()
    assert "unrecoverable" in (module.__doc__ or "").lower() or (
        "UNRECOVERABLE" in (module.__doc__ or "")
    )


def test_skips_backfill_when_projects_table_is_absent() -> None:
    """Fresh-DB / dry-run-mock safety: migration 051 (which creates
    `projects`) always runs before 074 in a real deploy, but the probe keeps
    this migration safe to execute standalone against a mocked client too.
    """
    module = _load()
    client = _FakeClient(projects_exists=False)
    module.upgrade(client)  # must not raise
    assert not any(
        "INSERT INTO project_declared_state_history" in c for c in client.commands
    )


def test_upgrade_is_idempotent_against_a_second_run() -> None:
    """No exception, and the CREATE/INSERT shape is identical the second
    time -- ReplacingMergeTree collapse (not migration-side guarding) is what
    makes re-running safe, matching migration 048's own documented pattern.
    """
    module = _load()
    client = _FakeClient(projects_exists=True)
    module.upgrade(client)
    first_commands = list(client.commands)
    module.upgrade(client)
    second_commands = client.commands[len(first_commands) :]
    assert first_commands == second_commands
