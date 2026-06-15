"""Fail-closed tests for migration 048 (CHAOS-2433 round-6).

Migration 048 seeds the legacy completion marker. Its table-existence probe must
fail CLOSED: an unexpected system.tables error must PROPAGATE (so the seed
migration is NOT recorded as applied without seeding), while a SUCCESSFUL
zero-row probe is the genuine fresh-DB / dry-run no-op skip.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
)
MIGRATION_048 = "048_seed_legacy_membership_run.py"


def _load() -> ModuleType:
    path = MIGRATIONS_DIR / MIGRATION_048
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Result:
    def __init__(self, rows: list[list[Any]]) -> None:
        self.result_rows = rows


class _FakeClient:
    def __init__(
        self, *, exists_raises: Exception | None = None, present: bool = True
    ) -> None:
        self._exists_raises = exists_raises
        self._present = present
        self.commands: list[str] = []

    def query(self, q: str, parameters: dict | None = None) -> _Result:
        if "FROM system.tables" in q:
            if self._exists_raises is not None:
                raise self._exists_raises
            return _Result([[1 if self._present else 0]])
        return _Result([])

    def command(self, cmd: str, parameters: dict | None = None) -> None:
        self.commands.append(cmd)


def test_existence_probe_error_propagates() -> None:
    """An unexpected probe error must RAISE (not be swallowed into a silent skip
    that marks the seed migration applied without seeding)."""
    module = _load()
    client = _FakeClient(exists_raises=RuntimeError("probe boom"))
    with pytest.raises(RuntimeError, match="probe boom"):
        module.upgrade(client)
    # No INSERT was attempted.
    assert not any(
        "INSERT INTO work_unit_membership_runs" in c for c in client.commands
    )


def test_zero_row_probe_is_genuine_skip() -> None:
    """A SUCCESSFUL zero-row probe (fresh DB / dry-run mock) is a clean no-op."""
    module = _load()
    client = _FakeClient(present=False)
    module.upgrade(client)  # must not raise
    assert not any(
        "INSERT INTO work_unit_membership_runs" in c for c in client.commands
    )


def test_present_tables_seed_marker() -> None:
    """When both tables exist, the legacy marker INSERT is issued."""
    module = _load()
    client = _FakeClient(present=True)
    module.upgrade(client)
    assert any("INSERT INTO work_unit_membership_runs" in c for c in client.commands)
