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


def test_history_table_version_column_is_version_key_not_last_synced() -> None:
    """Codex cross-system review C1 (HIGH): `updated_at`/`last_synced` are
    both only millisecond-precision -- two DIFFERENT observed states
    sharing the same millisecond for BOTH used to tie on the RMT version
    column (`last_synced`) with no further tie-break, so a pre-merge read
    could disagree with whatever a background merge eventually kept.
    `version_key` -- a MATERIALIZED value bit-packing `last_synced`
    (primary ordering, preserving F6's established semantic unchanged
    whenever `last_synced` differs) with a `cityHash64` of the declared
    CONTENT columns (tertiary tie-break, reached only when `last_synced`
    ALSO ties exactly) -- is the version column now.

    NOT `write_seq`/`generateSnowflakeID()` -- an earlier version of this
    fix used that, and the verifier measured its low 22 bits (a
    per-millisecond counter that RESETS every millisecond) colliding far
    more than a uniformly-distributed tie-break would, in 10 of 20 trials
    at ~100ms spacing. A content hash does not have that structured,
    non-uniform bit layout.
    """
    module = _load()
    client = _FakeClient()
    module.upgrade(client)
    create_stmt = next(c for c in client.commands if "CREATE TABLE" in c)
    normalized = " ".join(create_stmt.split())
    assert "cityHash64(" in normalized
    assert "version_key UInt64 MATERIALIZED" in normalized
    assert "ReplacingMergeTree(version_key)" in normalized
    assert "ReplacingMergeTree(last_synced)" not in normalized
    assert "generateSnowflakeID" not in normalized, (
        "the low-bit tie-break must be content-derived, not an id "
        "generator's low bits (measured to collide far more than assumed)"
    )


def test_backfills_from_projects_final_when_projects_exists() -> None:
    module = _load()
    client = _FakeClient(projects_exists=True)
    module.upgrade(client)
    backfill = [c for c in client.commands if "INSERT INTO" in c]
    assert backfill, "expected a backfill INSERT when projects exists"
    assert "FROM projects FINAL" in backfill[0]


def test_creates_a_separate_floor_table_written_only_by_this_migration() -> None:
    """PR #1602 review F4, corrected by round-2 review NEW-1 (HIGH,
    BLOCKS): the floor fact used to live in an `is_backfill_floor` column
    on `project_declared_state_history` itself -- but that table's RMT
    version column is `last_synced`, so an ORDINARY re-sync of an
    unchanged project (same `updated_at`, fresher `last_synced`,
    `is_backfill_floor = 0` by every ordinary writer's default) collapses
    the floor-seeded row away on the next merge. `project_declared_state_
    floor` is a SEPARATE table, written ONLY here -- no ordinary sync ever
    touches it, so a floor row can never be merged away.
    """
    module = _load()
    client = _FakeClient()
    module.upgrade(client)
    assert any(
        "CREATE TABLE IF NOT EXISTS project_declared_state_floor" in c
        for c in client.commands
    )
    create_stmt = next(
        c
        for c in client.commands
        if "CREATE TABLE" in c and "project_declared_state_floor" in c
    )
    normalized = " ".join(create_stmt.split())
    assert "ORDER BY (org_id, provider, id)" in normalized, (
        "the floor table's key deliberately excludes updated_at -- there is "
        "exactly one floor instant per (org_id, provider, id), unlike the "
        "history table's finer (org_id, provider, id, updated_at) key"
    )
    assert "floor_updated_at" in normalized


def test_backfill_seeds_the_floor_table_from_projects_final() -> None:
    module = _load()
    client = _FakeClient(projects_exists=True)
    module.upgrade(client)
    floor_backfill = next(
        c for c in client.commands if "INSERT INTO project_declared_state_floor" in c
    )
    normalized = " ".join(floor_backfill.split())
    assert "FROM projects FINAL" in normalized
    assert "org_id, provider, id, updated_at" in normalized


def test_skips_floor_backfill_when_projects_table_is_absent() -> None:
    module = _load()
    client = _FakeClient(projects_exists=False)
    module.upgrade(client)  # must not raise
    assert not any(
        "INSERT INTO project_declared_state_floor" in c for c in client.commands
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
