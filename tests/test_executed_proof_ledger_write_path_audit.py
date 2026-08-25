"""Absence audit for the executed-proof ledger's writers (CHAOS-4114).

The ledger is only as true as the set of code paths that maintain it, and
that set is invisible: nothing about adding a new ``sync_run_units`` writer
makes you think about a second table. The failure is silent and asymmetric --

* a missed ATTEMPTED write (a new INSERT path) makes the gate read a live pair
  as never-attempted and bootstrap it through forever. Fail OPEN. This is the
  exact CHAOS-4048/4049 shape the gate exists to catch, reintroduced through
  the gate's own evidence source.
* a missed PROVEN write (a new success terminalizer) makes the gate block a
  working route. Fail closed and self-healing, but still an outage-shaped
  surprise.

so this enumerates the writers from source and fails when the set changes.
The point is not that the current list is correct -- it is that GROWING the
list has to be a decision somebody made on purpose, in a diff, with this
docstring in front of them.

Deliberately NOT a runtime check. A runtime version could only observe the
paths a test happens to exercise, and the paths nobody exercises are the
whole problem.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "src" / "dev_health_ops"

#: Files allowed to INSERT a ``sync_run_units`` row. Each MUST record the pair
#: as attempted in the same transaction.
_ATTEMPTED_WRITERS = {
    "internal/scheduler/sync/materializer.go": "RecordExecutedProofAttempted",
    "src/dev_health_ops/sync/planner.py": "record_executed_proof_attempts",
    # CHAOS-4266: the executed-proof gate's synthetic cicd/deployments/
    # incidents/tests seeding constructs a SyncRunUnit already at SUCCESS
    # (no separate plan -> dispatch -> complete lifecycle), so it calls both
    # record_executed_proof_attempts AND record_executed_proof_terminal in
    # the same transaction as the insert -- see the comment at that call
    # site in processors/sync.py.
    "src/dev_health_ops/processors/sync.py": "record_executed_proof_attempts",
}

#: Files allowed to terminalize a unit as SUCCESS. Each MUST stamp the proven
#: bit in the same transaction.
_PROVEN_WRITERS = {
    "internal/providersync/repository_postgres.go": "RecordExecutedProofTerminal",
    "src/dev_health_ops/workers/sync_units.py": "record_executed_proof_terminal",
}


def _relative(path: Path) -> str:
    return path.relative_to(_REPO_ROOT).as_posix()


#: Directory prefixes discovery ignores. Note the repository itself may live
#: under a path containing any of these words, so the filter is applied to the
#: REPO-RELATIVE parts -- filtering on absolute parts silently discovered
#: nothing at all when this ran from a worktree, and a guard that finds no
#: files passes exactly like a guard that finds no problems.
_IGNORED_ROOTS = frozenset({".venv", "vendor", "node_modules"})


def _go_sources() -> list[Path]:
    return [
        path
        for path in _REPO_ROOT.glob("**/*.go")
        if not path.name.endswith("_test.go")
        and not _IGNORED_ROOTS.intersection(path.relative_to(_REPO_ROOT).parts)
    ]


def _go_raw_strings(source: str) -> list[str]:
    """The backtick-delimited literals, which is where Go keeps its SQL.

    Scanning whole files instead lets an unrelated statement elsewhere in the
    file satisfy a two-part match, which is how the first version of this
    guard missed repository_postgres.go: several UPDATE statements share the
    file and the naive regex spanned from one literal into another.
    """

    return source.split("`")[1::2]


def test_every_sync_run_unit_insert_path_records_the_attempt() -> None:
    inserting = {
        _relative(path)
        for path in _go_sources()
        if re.search(r"INSERT\s+INTO\s+public\.sync_run_units", path.read_text("utf-8"))
    }
    inserting |= {
        _relative(path)
        for path in _SRC.glob("**/*.py")
        if re.search(r"\bSyncRunUnit\(\s*$", path.read_text("utf-8"), re.MULTILINE)
    }
    expected = {name for name in _ATTEMPTED_WRITERS if not name.startswith("tests/")}
    assert inserting == expected, (
        "the set of paths that INSERT sync_run_units rows changed.\n"
        f"  found:    {sorted(inserting)}\n"
        f"  expected: {sorted(expected)}\n"
        "A new insert path must call the executed-proof ledger's ATTEMPTED write "
        "in the same transaction, then be added to _ATTEMPTED_WRITERS here. "
        "Without it the gate reads that pair as never-attempted and bootstraps "
        "it through forever -- it fails OPEN, invisibly (CHAOS-4114)."
    )
    for name, writer in _ATTEMPTED_WRITERS.items():
        assert writer in (_REPO_ROOT / name).read_text("utf-8"), (
            f"{name} inserts sync_run_units rows but no longer calls {writer}"
        )


def test_every_success_terminalizer_stamps_the_proof() -> None:
    terminalizing = {
        _relative(path)
        for path in _go_sources()
        for statement in _go_raw_strings(path.read_text("utf-8"))
        if re.search(r"UPDATE\s+public\.sync_run_units", statement)
        and re.search(r"SET\s+status\s*=\s*'success'", statement)
    }
    terminalizing |= {
        _relative(path)
        for path in _SRC.glob("**/*.py")
        # An UPDATE of the unit row itself, not a read model or a projection
        # that happens to mention the SUCCESS constant.
        if re.search(
            r"update\(SyncRunUnit\)[\s\S]{0,6000}?"
            r"status=SyncRunUnitStatus\.SUCCESS\.value",
            path.read_text("utf-8"),
        )
    }
    assert terminalizing == set(_PROVEN_WRITERS), (
        "the set of paths that terminalize a sync unit as SUCCESS changed.\n"
        f"  found:    {sorted(terminalizing)}\n"
        f"  expected: {sorted(_PROVEN_WRITERS)}\n"
        "A new success terminalizer must stamp the executed-proof ledger in the "
        "same transaction, then be added to _PROVEN_WRITERS here. Without it a "
        "working route reads as attempted-but-unproven and stops planning."
    )
    for name, writer in _PROVEN_WRITERS.items():
        assert writer in (_REPO_ROOT / name).read_text("utf-8"), (
            f"{name} terminalizes units as success but no longer calls {writer}"
        )


def test_failure_terminalizers_are_deliberately_not_ledger_writers() -> None:
    """The sweep and the repairs cannot move either bit -- state it, in code.

    Their units already exist, so the pair is already attempted, and a failure
    never proves anything. This is the reasoning that keeps them off the lists
    above; if one of them ever DID gain a ledger write, that would mean the
    reasoning changed and this guard should be the thing that says so.
    """

    for name in (
        "internal/syncreconciler/unreclaimable_sweep.go",
        "internal/syncreconciler/terminal_delivery_repair.go",
        "internal/joboutbox/terminal_delivery_repair.go",
        "src/dev_health_ops/sync/budget_guard.py",
    ):
        source = (_REPO_ROOT / name).read_text("utf-8")
        assert "ExecutedProof" not in source and "executed_proof" not in source, (
            f"{name} now touches the executed-proof ledger. Failure "
            "terminalization cannot change attempted (the row already exists) or "
            "proven (a failure proves nothing), so either this is a no-op write "
            "worth deleting or the ledger's semantics changed and this guard, "
            "the writer lists above, and the migration comment all need revising."
        )
