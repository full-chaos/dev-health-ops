"""CHAOS-3219 Codex adversarial review (HIGH-5, 2026-08-05) -- full history:

1. ORIGINAL bug: ``age_source_rows`` was delete-then-reinsert. If the
   process died between those two ClickHouse calls (crash, OOM-kill,
   network partition), the affected table was left with ZERO rows for that
   org/repo, which ``DataHealthService.inspect`` reads as
   ``DataHealthState.NO_DATA``, silently corrupting a "stale" claim into a
   "no data" one.

2. FIRST fix attempt (same day): reorder to insert-then-delete, reasoning
   that the worst a crash could do is leave both old and aged rows present,
   which a ReplacingMergeTree resolves harmlessly via FINAL.

3. That reasoning was WRONG, caught live: every affected table is a
   ``ReplacingMergeTree(<watermark column>)``, and "aging" a row means
   writing a LOWER version value than what's already there -- exactly the
   case ReplacingMergeTree's "highest version wins" merge rule resolves the
   wrong way. A live two-generation run caught this directly via
   ``system.part_log`` forensics on a real scratch ClickHouse: the aged
   INSERT landed (``NewPart``, 39 rows), a background merge started ~300
   MICROSECONDS later and finished in ~2.4ms, and the merge kept the
   pre-existing higher-version (un-aged) rows -- silently, with NO crash
   and NO exception, under completely normal execution. Insert-then-delete
   is strictly worse than the bug it was meant to fix: the corruption no
   longer needs a crash to happen, and the postcondition check is the only
   reason it was noticed at all rather than shipping as an invisible bug.

4. CORRECTED fix (this file describes what's actually shipped): back to
   delete-then-insert, which has no competing higher-version row left for a
   merge to prefer once the delete completes, plus the postcondition check
   from step 2 kept as a genuine improvement over the ORIGINAL code (which
   had zero verification of any kind). The one hazard that remains -- a
   crash strictly between the delete and the insert -- cannot be closed at
   the ClickHouse-statement level (no cross-statement transactions for
   MergeTree tables) and is bounded instead by the operating model:
   ``fixtures world`` runs only against disposable SCRATCH databases that
   are always regenerated fresh, never resumed in place after a failure.

This file proves, with a *stateful* stub ClickHouse client (one that
actually stores rows, not just records calls -- per the CLAUDE.md "assert
the state the system exists to reach" rule):

  - the happy path: delete runs before insert, ending with exactly the
    aged rows present;
  - a delete failure skips the insert entirely (original rows untouched);
  - an insert that "succeeds" but isn't confirmed durable raises loudly
    instead of being silently accepted (this table is empty at that point
    -- the honestly-documented residual gap, now at least LOUD rather than
    silent);
  - the REJECTED design's failure mode reproduced directly: even with no
    crash at all, insert-then-delete loses the aged write to a
    ReplacingMergeTree merge that prefers the higher (un-aged) version --
    proving this file would have caught the bug that live testing caught,
    not just exercising the code that replaced it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from dev_health_ops.fixtures.generators import source_health

_ORG = "org-1"
_REPO = "repo-1"
_OLD_WATERMARK = datetime(2026, 1, 1, tzinfo=timezone.utc)
_STALE_WATERMARK = datetime(2020, 6, 1, tzinfo=timezone.utc)

_COLUMNS = ["repo_id", "hash", "org_id", "last_synced"]


def _seed_row() -> list[Any]:
    return [_REPO, "deadbeef", _ORG, _OLD_WATERMARK]


class _StubResult:
    def __init__(self, column_names: list[str], rows: list[list[Any]]) -> None:
        self.column_names = column_names
        self.result_rows = [tuple(row) for row in rows]


class _StatefulStubClient:
    """Actually stores rows -- ``insert`` appends, ``command`` (DELETE)
    empties or filters, ``query`` reads back current state. This is what
    makes the row-count assertions below a measurement of real state, not
    just a check that the right methods were called."""

    def __init__(self, rows: list[list[Any]]) -> None:
        self.rows: list[list[Any]] = [list(r) for r in rows]
        self.column_names = list(_COLUMNS)
        self.watermark_idx = self.column_names.index("last_synced")
        #: each entry is (table, rows_snapshot, column_names, rows_in_table_before)
        #: -- the last element lets tests prove ORDERING structurally (e.g.
        #: "the table was already empty when insert() ran"), not just count
        #: how many times each method was called.
        self.insert_calls: list[tuple[Any, ...]] = []
        self.command_calls: list[tuple[str, int]] = []
        self.raise_on_insert: Exception | None = None
        self.raise_on_command: Exception | None = None
        #: if set, the count-check query returns this instead of the true
        #: count -- simulates a write whose durability hasn't been confirmed.
        self.force_observed_count: int | None = None

    def query(self, sql: str, parameters: dict[str, Any]) -> _StubResult:
        if "count()" in sql:
            if self.force_observed_count is not None:
                return _StubResult(["count"], [[self.force_observed_count]])
            stale = parameters["stale_watermark"]
            count = sum(1 for r in self.rows if r[self.watermark_idx] == stale)
            return _StubResult(["count"], [[count]])
        return _StubResult(self.column_names, self.rows)

    def insert(
        self, table: str, rows: list[list[Any]], column_names: list[str]
    ) -> None:
        self.insert_calls.append(
            (table, [list(r) for r in rows], column_names, len(self.rows))
        )
        if self.raise_on_insert is not None:
            raise self.raise_on_insert
        self.rows.extend(list(r) for r in rows)

    def command(self, sql: str, parameters: dict[str, Any]) -> None:
        self.command_calls.append((sql, len(self.rows)))
        if self.raise_on_command is not None:
            raise self.raise_on_command
        if "stale_watermark" in parameters:
            # The REJECTED insert-then-delete design's delete: exclude the
            # already-inserted aged rows, remove only the superseded
            # originals. Not exercised by the shipped code path, kept only
            # so the rejected-design reference tests below can drive this
            # stub the same way that design would have.
            stale = parameters["stale_watermark"]
            self.rows = [r for r in self.rows if r[self.watermark_idx] == stale]
        else:
            # The SHIPPED delete-then-insert design's delete: unconditional
            # on org_id/repo_id alone (every row this stub ever holds
            # already matches those, by construction).
            self.rows = []


async def _age(client: Any) -> None:
    await source_health.age_source_rows(
        client,
        org_id=_ORG,
        repo_id=_REPO,
        source="commits",
        stale_watermark=_STALE_WATERMARK,
    )


class TestAgeSourceRowsHappyPath:
    @pytest.mark.asyncio
    async def test_ends_with_only_aged_rows(self) -> None:
        client = _StatefulStubClient([_seed_row()])
        await _age(client)
        assert len(client.rows) == 1
        assert client.rows[0][client.watermark_idx] == _STALE_WATERMARK

    @pytest.mark.asyncio
    async def test_delete_happens_before_insert(self) -> None:
        client = _StatefulStubClient([_seed_row()])
        await _age(client)
        assert len(client.command_calls) == 1
        assert len(client.insert_calls) == 1
        # Structural proof, not a call-count coincidence: at the moment
        # insert() ran, the table had already been emptied by the (earlier)
        # delete call.
        _table, _rows, _cols, rows_in_table_before_insert = client.insert_calls[0]
        assert rows_in_table_before_insert == 0, (
            "insert() must run AFTER delete() -- this is the corrected "
            "ordering: no competing higher-version row may be present when "
            "the aged rows are written, or a ReplacingMergeTree merge can "
            "silently prefer the wrong one (see module docstring)"
        )

    @pytest.mark.asyncio
    async def test_no_rows_is_a_noop(self) -> None:
        client = _StatefulStubClient([])
        await _age(client)
        assert client.insert_calls == []
        assert client.command_calls == []


class TestAgeSourceRowsDeleteFailure:
    @pytest.mark.asyncio
    async def test_delete_exception_propagates_and_skips_insert(self) -> None:
        client = _StatefulStubClient([_seed_row()])
        client.raise_on_command = RuntimeError("simulated ClickHouse delete failure")
        with pytest.raises(RuntimeError, match="simulated ClickHouse delete failure"):
            await _age(client)
        assert client.insert_calls == [], (
            "insert must never run if the delete raised -- the original "
            "row must be untouched"
        )
        assert len(client.rows) == 1
        assert client.rows[0][client.watermark_idx] == _OLD_WATERMARK


class TestAgeSourceRowsUnconfirmedInsert:
    @pytest.mark.asyncio
    async def test_short_observed_count_raises_loudly(self) -> None:
        client = _StatefulStubClient([_seed_row()])
        # The delete already ran (this design's first step) and the insert
        # "succeeds" (appends normally), but the read-your-write count
        # check is forced to see fewer rows than were just inserted --
        # simulating a write whose durability hasn't been confirmed yet.
        client.force_observed_count = 0
        with pytest.raises(
            source_health.SourceAgingWriteError, match="not confirmed durable"
        ):
            await _age(client)
        # This IS the honestly-documented residual gap: the original row
        # is already gone (delete ran first) and the new one is
        # unconfirmed. Nothing can retroactively make this atomic -- what
        # this postcondition buys is that the operation FAILS LOUDLY here
        # instead of silently reporting success over an unverified write.
        assert len(client.rows) == 1
        assert client.rows[0][client.watermark_idx] == _STALE_WATERMARK


class TestOrderingRationale:
    """Proves the actual tradeoff ``age_source_rows``'s docstring describes,
    for BOTH orderings, against the stateful stub -- so neither claim is
    prose the suite doesn't verify."""

    @pytest.mark.asyncio
    async def test_shipped_ordering_crash_between_steps_reaches_zero_rows(
        self,
    ) -> None:
        """The one hazard delete-then-insert cannot avoid: a process crash
        landing strictly between the DELETE and the INSERT leaves zero
        rows. Proven here (not hidden) so nobody mistakes this suite for
        claiming a guarantee ClickHouse cannot actually provide -- the
        function's docstring explains why this residual window is accepted
        (bounded by the scratch-database, regenerate-fresh operating
        model) rather than pretended away."""

        client = _StatefulStubClient([_seed_row()])
        client.raise_on_insert = RuntimeError(
            "simulated crash between DELETE and INSERT"
        )

        with pytest.raises(
            RuntimeError, match="simulated crash between DELETE and INSERT"
        ):
            await _age(client)

        assert len(client.rows) == 0, (
            "a crash strictly between delete and insert does leave zero "
            "rows under the shipped ordering -- this is the known, "
            "documented residual gap, not a claim this test disproves"
        )

    def test_rejected_insert_then_delete_loses_the_aged_write_to_a_merge(
        self,
    ) -> None:
        """RED proof for the REJECTED design, reproducing what live
        ClickHouse forensics caught: with insert-then-delete, even with NO
        crash at all, the aged replacement can lose to a background
        ReplacingMergeTree merge that always keeps the row with the HIGHER
        version column -- and "aging" always writes a LOWER value. A stub
        cannot run ClickHouse's real background merge scheduler, so this
        models the merge's documented outcome directly: independently
        confirmed live (see ``age_source_rows``'s docstring) via
        ``system.part_log``, where a real merge started ~300us after the
        insert and completed in ~2.4ms."""

        client = _StatefulStubClient([_seed_row()])

        # The rejected design's insert step: the aged row is written
        # WITHOUT deleting the original first, so both versions coexist,
        # pre-merge.
        aged = list(_seed_row())
        aged[client.watermark_idx] = _STALE_WATERMARK
        client.rows.append(aged)
        assert len(client.rows) == 2

        # The merge: ReplacingMergeTree keeps the row with the MAX version
        # per sorting key. Since aging always moves the watermark
        # BACKWARD, the aged replacement always loses this race.
        winner = max(client.rows, key=lambda r: r[client.watermark_idx])
        client.rows = [winner]

        assert winner[client.watermark_idx] == _OLD_WATERMARK, (
            "the merge keeps the row with the numerically LARGER "
            "watermark -- the aged (numerically smaller) replacement is "
            "silently discarded, with no crash and no exception required. "
            "This is why insert-then-delete was rejected: it is not "
            "merely 'also has a crash window', it corrupts data on "
            "completely normal, successful execution."
        )
