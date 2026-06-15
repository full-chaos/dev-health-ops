"""Fail-closed tests for migration 049 (CHAOS-2433 round-6).

Migration 049 rebuilds work_unit_membership so run_id is in the ReplacingMergeTree
dedup key. A structural migration must NEVER be recorded as applied without
actually rebuilding the key — otherwise the round-2 background-merge eviction
silently returns (a merge collapses across run_ids and removes the still-visible
complete generation). These tests prove, with a fake client (no live DB):

- An UNEXPECTED error from the existence probe PROPAGATES → upgrade() RAISES
  (so the runner does NOT record 049 as applied; it can be retried).
- A SUCCESSFUL existence probe returning ZERO rows is a genuine skip (fresh DB /
  dry-run no-op), NOT an error.
- Post-rebuild sorting-key verification fails closed: if run_id is not last in
  the LIVE main-table key after the rebuild attempt, upgrade() RAISES.
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
MIGRATION_049 = "049_work_unit_membership_run_id_dedup_key.py"

_OLD_KEY = "org_id, node_type, node_id, category_kind, category"
_NEW_KEY = "org_id, node_type, node_id, category_kind, category, run_id"
_TABLE = "work_unit_membership"


def _load() -> ModuleType:
    path = MIGRATIONS_DIR / MIGRATION_049
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Result:
    def __init__(self, rows: list[list[Any]]) -> None:
        self.result_rows = rows


class _FakeClient:
    """Drives migration 049's queries. Configurable per-scenario.

    - ``exists_raises``: the system.tables existence probe raises this exception.
    - ``table_present``: count() result for the existence probe (0 => absent).
    - ``sorting_key``: the LIVE sorting key returned for the main table; updated
      to the new key after a simulated EXCHANGE so the post-rebuild check passes.
    - ``verify_stays_old``: if True, the main-table sorting key NEVER gains run_id
      even after EXCHANGE (simulates a rebuild that did not land) so the
      post-rebuild verification must RAISE.
    """

    def __init__(
        self,
        *,
        exists_raises: Exception | None = None,
        table_present: bool = True,
        verify_stays_old: bool = False,
    ) -> None:
        self._exists_raises = exists_raises
        self._table_present = table_present
        self._verify_stays_old = verify_stays_old
        self._main_key = _OLD_KEY
        self._shadow_exists = False
        self.commands: list[str] = []

    def query(self, q: str, parameters: dict | None = None) -> _Result:
        params = parameters or {}
        if "FROM system.tables" in q and "count()" in q:
            if self._exists_raises is not None:
                raise self._exists_raises
            name = params.get("name")
            if name == "work_unit_membership_new":
                return _Result([[1 if self._shadow_exists else 0]])
            return _Result([[1 if self._table_present else 0]])
        if "sorting_key" in q:
            name = params.get("name")
            if name == "work_unit_membership_new":
                # Shadow always built with the new key.
                return _Result([[_NEW_KEY]])
            return _Result([[self._main_key]])
        if "SHOW CREATE TABLE" in q:
            ddl = (
                f"CREATE TABLE {_TABLE} (org_id String, node_type String, "
                "node_id String, category_kind String, category String, "
                "run_id String) ENGINE = ReplacingMergeTree(computed_at) "
                f"ORDER BY ({_OLD_KEY})"
            )
            return _Result([[ddl]])
        if "uniqExact" in q:
            return _Result([[0]])
        return _Result([])

    def command(self, cmd: str, parameters: dict | None = None) -> None:
        self.commands.append(cmd)
        if cmd.startswith("EXCHANGE TABLES"):
            # The verified shadow becomes the main table — its key is the new key,
            # UNLESS the scenario simulates a rebuild that did not land.
            if not self._verify_stays_old:
                self._main_key = _NEW_KEY


def test_existence_probe_error_propagates_not_skipped() -> None:
    """An unexpected existence-probe error must RAISE (so 049 is NOT recorded as
    applied), not be swallowed into a silent 'table absent' skip."""
    module = _load()
    client = _FakeClient(exists_raises=RuntimeError("transient probe failure"))

    with pytest.raises(RuntimeError, match="transient probe failure"):
        module.upgrade(client)

    # The migration must NOT have proceeded to any rebuild command.
    assert not any("EXCHANGE TABLES" in c for c in client.commands)


def test_restricted_user_probe_error_propagates() -> None:
    """A permission error from the probe also propagates (fail closed)."""
    module = _load()

    class _AccessDenied(Exception):
        pass

    client = _FakeClient(exists_raises=_AccessDenied("not enough privileges"))
    with pytest.raises(_AccessDenied):
        module.upgrade(client)


def test_successful_zero_row_probe_is_genuine_skip() -> None:
    """A SUCCESSFUL probe returning zero rows => fresh-DB no-op skip (no rebuild,
    no error). This is the idempotent / dry-run path that must still work."""
    module = _load()
    client = _FakeClient(table_present=False)

    # Must NOT raise and must NOT attempt a rebuild.
    module.upgrade(client)
    assert not any("EXCHANGE TABLES" in c for c in client.commands)


def test_post_rebuild_verification_fails_closed_when_key_missing_run_id() -> None:
    """If, after the rebuild attempt, the LIVE main-table key still lacks run_id,
    upgrade() must RAISE (refuse to be marked applied)."""
    module = _load()
    # table present, old key, but the EXCHANGE does not install the new key.
    client = _FakeClient(table_present=True, verify_stays_old=True)

    with pytest.raises(RuntimeError, match="post-rebuild verification failed"):
        module.upgrade(client)


def test_successful_rebuild_completes() -> None:
    """A healthy rebuild path completes: the post-rebuild verification sees the
    new key (run_id last) and upgrade() returns without raising."""
    module = _load()
    client = _FakeClient(table_present=True, verify_stays_old=False)

    module.upgrade(client)  # must not raise
    # The atomic swap was issued.
    assert any("EXCHANGE TABLES" in c for c in client.commands)


def test_already_migrated_is_idempotent_skip() -> None:
    """If run_id is already last in the key, upgrade() skips (no rebuild)."""
    module = _load()
    client = _FakeClient(table_present=True)
    client._main_key = _NEW_KEY  # already migrated

    module.upgrade(client)
    assert not any("EXCHANGE TABLES" in c for c in client.commands)


def test_magicmock_client_is_treated_as_absent_not_crash() -> None:
    """REGRESSION (CI red, run 27579877858): a bare MagicMock client (e.g.
    test_teams::test_clickhouse_store_teams drives ALL migrations through
    ClickHouseStore.__aenter__ with a MagicMock) returns a successful-but-
    uninterpretable result_rows. The round-6 fail-closed change must NOT crash on
    it: a successful probe with a non-list/non-int shape is treated as absent
    (genuine skip), while only a real query EXCEPTION fails closed. int(MagicMock)
    deceptively yields 1, so the interpretation must be type-strict."""
    from unittest.mock import MagicMock

    module = _load()
    client = MagicMock()  # query() returns a MagicMock; .result_rows is a MagicMock

    # Must NOT raise (no TypeError from '>' or regex on a MagicMock) and must NOT
    # attempt a rebuild — the migration treats the mock as 'table absent' and skips.
    module.upgrade(client)
    issued = [c.args[0] for c in client.command.call_args_list if c.args]
    assert not any("EXCHANGE TABLES" in c for c in issued)


def test_count_gt_zero_is_type_strict() -> None:
    """_count_gt_zero accepts only a real list/tuple of rows with an int count;
    anything else (MagicMock, non-numeric, empty, bool) is False/absent."""
    from unittest.mock import MagicMock

    module = _load()
    assert module._count_gt_zero([[1]]) is True
    assert module._count_gt_zero([(5,)]) is True
    assert module._count_gt_zero([[0]]) is False
    assert module._count_gt_zero([]) is False
    assert module._count_gt_zero(None) is False
    assert module._count_gt_zero(MagicMock()) is False  # the CI-red shape
    assert module._count_gt_zero([[MagicMock()]]) is False
    assert module._count_gt_zero([["3"]]) is False  # non-int cell
    assert module._count_gt_zero([[True]]) is False  # bool excluded
