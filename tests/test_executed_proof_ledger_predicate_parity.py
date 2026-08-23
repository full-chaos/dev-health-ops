"""The executed-proof verdict must have exactly ONE definition (CHAOS-4114).

Three things decide whether a terminal ``sync_run_units`` row counts as live
executed proof: the Go per-unit stamp written on terminalization, the alembic
0109 one-time backfill, and -- as the oracle the other two are checked
against -- the pre-ledger whole-table scan. If any of them drifts, the gate
starts blocking or admitting routes for reasons nobody wrote down, and the
symptom (a provider that quietly stops planning) looks nothing like the cause.

Go holds the definition, in ``executedProofProvenPredicateSQL``. The Go
writers get it by construction, because they concatenate that constant. The
migration cannot, so this asserts it carries the identical expression.

Whitespace is normalized because the two files indent differently and nothing
about SQL semantics depends on that; everything else must match byte for byte.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_GO_SOURCE = _REPO_ROOT / "internal" / "providersync" / "executed_proof.go"
_MIGRATION = (
    _REPO_ROOT
    / "src"
    / "dev_health_ops"
    / "alembic"
    / "versions"
    / "0109_add_sync_executed_proof_ledger.py"
)


def _collapse(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _go_constant(name: str) -> str:
    source = _GO_SOURCE.read_text(encoding="utf-8")
    match = re.search(rf"const {name} = `(.*?)`", source, re.DOTALL)
    assert match is not None, (
        f"{name} is gone from {_GO_SOURCE.name}. It is the single definition of "
        "the executed-proof verdict; if it was renamed, point this guard at the "
        "new name rather than deleting the guard."
    )
    return match.group(1)


def _go_declaration(name: str) -> str:
    """The whole ``const <name> = ...`` declaration, concatenations included.

    Not just the first backtick literal: these constants are built by
    concatenating the shared predicate between two raw-string halves, and the
    reference this guard exists to check lives BETWEEN them.
    """

    source = _GO_SOURCE.read_text(encoding="utf-8")
    start = source.index(f"const {name} = ")
    tail = source[start + len(f"const {name} = ") :]
    # A declaration ends at the first blank line that is not inside a raw
    # string. Counting backticks is enough here because none of these
    # constants contains one.
    end, backticks = 0, 0
    for line in tail.splitlines(keepends=True):
        backticks += line.count("`")
        end += len(line)
        if backticks % 2 == 0 and not line.strip().endswith("+"):
            break
    return tail[:end]


def test_the_migration_backfill_evaluates_the_go_proven_predicate_verbatim() -> None:
    predicate = _collapse(_go_constant("executedProofProvenPredicateSQL"))
    # A predicate that collapsed to something trivial would make every
    # assertion below pass for the wrong reason.
    assert "unit.status = 'success'" in predicate
    assert "go_provider_route,records" in predicate
    assert "persisted" in predicate
    assert "^[0-9]{1,18}$" in predicate

    migration = _collapse(_MIGRATION.read_text(encoding="utf-8"))
    assert predicate in migration, (
        "alembic 0109's backfill no longer evaluates the same proven-predicate "
        "expression as internal/providersync/executed_proof.go. A backfilled "
        "ledger would then disagree with every row the runtime stamps after it, "
        "and the disagreement is silent: routes simply stop planning."
    )


def test_the_legacy_scan_and_both_ledger_writers_share_the_one_predicate() -> None:
    """The Go side gets parity by construction -- prove it still does.

    Each of these declarations must EMBED the shared constant rather than
    restate the expression. A future edit that pasted it inline would compile,
    pass every behavioral test, and quietly create a second definition to
    drift from.
    """

    for constant in (
        "executedProofEvidenceSQL",
        "executedProofLedgerTerminalSQL",
        "ExecutedProofLedgerBackfillSQL",
    ):
        declaration = _go_declaration(constant)
        assert "executedProofProvenPredicateSQL" in declaration, (
            f"{constant} no longer references executedProofProvenPredicateSQL. "
            "It must concatenate the shared constant, never restate the "
            "expression: a second copy is a second definition."
        )


def test_the_ledger_read_does_not_scan_sync_run_units() -> None:
    """The point of the ticket, asserted directly.

    CHAOS-4124 was caused by the evidence refresh being a whole-table scan of
    a table that only grows. A change that pointed the refresh back at
    ``sync_run_units`` -- for any reason, however locally sensible -- would
    reintroduce the outage, so it fails here.
    """

    read = _go_constant("executedProofLedgerReadSQL")
    assert "sync_executed_proof_ledger" in read
    assert "sync_run_units" not in read

    source = _GO_SOURCE.read_text(encoding="utf-8")
    refresh = source.split("func QueryExecutedProofEvidence(")[1]
    assert "executedProofLedgerReadSQL" in refresh
    assert "executedProofEvidenceSQL" not in refresh, (
        "QueryExecutedProofEvidence is scanning sync_run_units again. That scan "
        "is what timed out at scheduler startup on 2026-08-22 and blocked every "
        "non-waived route for eight hours (CHAOS-4124)."
    )
