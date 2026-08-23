"""Writers for the durable executed-proof ledger (CHAOS-4114).

The CHAOS-4060 route-readiness gate used to derive its evidence from a
whole-table ``GROUP BY`` over ``sync_run_units`` with per-row JSON extraction.
That table only grows, the scan outgrew both of its deadlines, and on
2026-08-22 a timed-out startup refresh installed an empty Degraded snapshot
that blocked every non-waived route from planning for eight hours
(CHAOS-4124). ``sync_executed_proof_ledger`` is the maintained projection that
replaces the scan; this module is its Python write surface.

There are exactly two bits and they are written at two different moments:

``attempted_at``
    Stamped by whatever INSERTs the ``sync_run_units`` rows, in the SAME
    transaction. "Attempted" is a claim about row EXISTENCE -- the query being
    replaced had no ``WHERE`` clause, so a pair became attempted the instant
    planning minted its first row, whatever became of it. Missing this write
    makes the gate read a live pair as never-attempted and bootstrap it
    through: the fail-OPEN direction, which this gate must never take.

``proven_at``
    Stamped by whatever terminalizes a unit SUCCESSFULLY with a positive
    persisted-row count, in the SAME transaction. Missing this write makes the
    gate read a working pair as attempted-but-unproven and BLOCK it: loud,
    fail-closed, and self-healing on the next success.

Both are monotone. Nothing in this system may un-prove a route -- that is what
``bool_or`` meant in the query being replaced, and the upserts here reproduce
it with ``COALESCE(existing, excluded)``.

The proven verdict itself is defined ONCE, in SQL, as
``executedProofProvenPredicateSQL`` in ``internal/providersync/executed_proof.go``
(the Go writers and the alembic 0109 backfill both evaluate that literal
expression). :func:`result_proves_execution` is this side's mirror of it, and
it is not trusted on inspection: ``tests/test_executed_proof_ledger_oracle.py``
runs both against a shared corpus on a real PostgreSQL and fails on any
disagreement.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from dev_health_ops.models import SyncExecutedProofLedger

__all__ = [
    "record_executed_proof_attempts",
    "record_executed_proof_terminal",
    "result_proves_execution",
]

#: Bounded to 18 digits for the same reason the SQL is: ``bigint`` tops out at
#: 19 digits, so an unbounded ``^[0-9]+$`` lets a 19+ digit value reach the
#: cast and raise "value out of range" -- which, in the SQL, failed the WHOLE
#: evidence query rather than just skipping the row. 18 digits is always in
#: range. Here it costs nothing and keeps the two implementations identical.
_BOUNDED_DIGITS = re.compile(r"^[0-9]{1,18}$")


def _json_text(value: Any) -> str | None:
    """Mirror PostgreSQL's ``->>`` / ``#>>`` text extraction.

    Both operators yield SQL NULL for an absent path and for JSON ``null``,
    the raw string for a JSON string, and the JSON rendering for everything
    else. The distinction matters because the caller then applies a
    digits-only regex: anything that renders as an object, an array, ``true``,
    or a float simply fails the regex, exactly as it does in SQL.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return repr(value) if isinstance(value, float) else str(value)
    return json.dumps(value)


def _positive_bounded_count(text: str | None) -> bool:
    if text is None or not _BOUNDED_DIGITS.match(text):
        return False
    return int(text) > 0


def result_proves_execution(result: Any) -> bool:
    """Whether a terminal unit ``result`` payload is live executed proof.

    The Python mirror of ``executedProofProvenPredicateSQL``'s
    payload half (the caller supplies the ``status = 'success'`` half). Two
    accepted shapes, and only two:

    * ``result.go_provider_route.records`` -- the Go completion payload's true
      per-row count. Deliberately NOT ``effects_written``, which counts
      committed effect BATCHES: a route can commit a batch containing zero
      rows, and proving on that would readmit the exact CHAOS-4049
      "succeeded but persisted nothing" shape this gate exists to catch.
    * ``result.persisted`` -- the legacy pre-cutover Python key.

    Anything else -- absent, null, non-numeric, negative, zero, or an
    out-of-range magnitude -- is not proof.
    """

    if not isinstance(result, Mapping):
        return False
    nested = result.get("go_provider_route")
    records = nested.get("records") if isinstance(nested, Mapping) else None
    if _positive_bounded_count(_json_text(records)):
        return True
    return _positive_bounded_count(_json_text(result.get("persisted")))


def _dialect_insert(session: Session) -> Any:
    """The dialect-specific ``insert`` that exposes ``on_conflict_*``.

    Same dispatch as ``sync/dispatch_outbox.py``'s upsert, for the same
    reason: the unit suite exercises these callers on in-memory SQLite while
    production is PostgreSQL, and both dialects expose the identical API.

    An unrecognized dialect raises rather than skipping. A silent skip on the
    attempted path would plan units whose attempt went unrecorded, and the
    gate reads an unrecorded pair as never-attempted -- it would fail OPEN,
    invisibly, which is the whole failure class this ledger exists to close.
    """

    dialect_name = session.get_bind().dialect.name
    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as postgresql_insert

        return postgresql_insert
    if dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        return sqlite_insert
    raise RuntimeError(
        "executed-proof ledger upsert has no implementation for SQL dialect "
        f"{dialect_name!r}; refusing to write sync state whose evidence would "
        "go unrecorded (CHAOS-4114)"
    )


def _normalized(provider: Any, dataset_key: Any) -> tuple[str, str] | None:
    """Lowercase and trim one pair, or ``None`` if either half is empty.

    The lowercase form is a CHECK-constrained INVARIANT of the table, not a
    convention: a writer that forgot it would mint a second, invisible row for
    the same pair and split one route's proof in half.
    """

    provider_text = str(provider or "").strip().lower()
    dataset_text = str(dataset_key or "").strip().lower()
    if not provider_text or not dataset_text:
        return None
    return provider_text, dataset_text


def record_executed_proof_attempts(
    session: Session,
    pairs: Sequence[tuple[Any, Any]],
    *,
    now: datetime,
) -> None:
    """Mark every supplied ``(provider, dataset_key)`` pair as ATTEMPTED.

    ``DO NOTHING``, never ``DO UPDATE``: ``attempted_at`` records when the pair
    FIRST became attempted, so the first writer wins -- and a re-plan must not
    clobber a ``proven_at`` an earlier completion already wrote.

    An empty pair list is a no-op rather than an error: a zero-unit plan is a
    legitimate outcome and must not fail the planning it belongs to.
    """

    normalized = sorted(
        {
            pair
            for pair in (_normalized(provider, dataset) for provider, dataset in pairs)
            if pair is not None
        }
    )
    if not normalized:
        return
    dialect_insert = _dialect_insert(session)
    statement = dialect_insert(SyncExecutedProofLedger.__table__).values(
        [
            {"provider": provider, "dataset_key": dataset, "attempted_at": now}
            for provider, dataset in normalized
        ]
    )
    session.execute(
        statement.on_conflict_do_nothing(index_elements=["provider", "dataset_key"])
    )
    session.flush()


def record_executed_proof_terminal(
    session: Session,
    *,
    provider: Any,
    dataset_key: Any,
    status: str,
    result: Any,
    now: datetime,
) -> None:
    """Stamp the ledger for a unit that just reached a TERMINAL state.

    Call it in the same transaction as the terminal write, after it.

    A failure terminalization is deliberately not an error here: it merely
    re-asserts ``attempted_at``, which is already set, because the predicate
    requires ``status='success'`` with a positive row count. That is also why
    the failure terminalizers elsewhere in the codebase (the unreclaimable
    sweep, the terminal-delivery repair, the budget-guard chokepoint) do not
    call this at all -- they cannot move either bit.

    ``proven_at`` is ``COALESCE``d so an already-proven pair keeps its original
    proving instant and a later unproven completion can never un-prove it.
    """

    pair = _normalized(provider, dataset_key)
    if pair is None:
        return
    provider_text, dataset_text = pair
    proven_at = (
        now if (status == "success" and result_proves_execution(result)) else None
    )
    dialect_insert = _dialect_insert(session)
    table = SyncExecutedProofLedger.__table__
    statement = dialect_insert(table).values(
        provider=provider_text,
        dataset_key=dataset_text,
        attempted_at=now,
        proven_at=proven_at,
    )
    session.execute(
        statement.on_conflict_do_update(
            index_elements=["provider", "dataset_key"],
            set_={
                "proven_at": func.coalesce(
                    table.c.proven_at, statement.excluded.proven_at
                )
            },
        )
    )
    session.flush()
