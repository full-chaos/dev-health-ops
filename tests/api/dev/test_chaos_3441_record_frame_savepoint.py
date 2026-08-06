"""CHAOS-3441: a mid-flush ``record_frame`` failure must roll back ONLY the
frame write.

Scope check first (this file is the closure evidence, so it states what was
already true on ``main``): ``DevPersistenceService.record_frame`` and
``record_narrative`` already isolate their flush in a SAVEPOINT
(``begin_nested``), landed with CHAOS-3423/3424 (#1507), and
``test_chaos_3423_record_frame_integrity_failure_never_poisons_the_session``
already proves the NO-ANSWER path survives a real duplicate-insert
``IntegrityError``. What this file adds is the rest of the ticket:

* the REAL-ANSWER path (an already-flushed ``DevAnswer`` transcript row --
  the pre-existing, accepted tradeoff CHAOS-3297 round 3 Finding 2
  documented and this ticket exists to close), driven by a
  connection-level mid-flush fault rather than a constraint violation, so
  the proof is not specific to ``IntegrityError``;
* the ORM half of "rolls back only the frame write". ``record_frame`` tags
  its owned run INSIDE the savepoint (``contract_generation = 'v2'``,
  ``public_outcome``) -- in-Python mutations on a *persistent* object, which
  a SAVEPOINT rollback does not undo by itself. Investigating that as a
  suspected leak REFUTED it: SQLAlchemy expires the attributes touched by
  the failed flush, so the next flush on the outer session does not re-emit
  the tags and no ``contract_generation = 'v2'`` run without a frame row is
  ever committed (the CHAOS-3299 / migration-0074 downgrade-guard shape).
  The second test below is therefore a lock on an invariant that already
  holds, not a bug fix -- and it is not vacuous: it fails when
  ``record_frame``'s ``begin_nested`` is removed.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev import terminal_frames as dev_terminal_frames
from dev_health_ops.api.dev.persistence import (
    DevPersistenceService,
    DevPersistenceValidationError,
)
from dev_health_ops.models.dev_persistence import (
    DevAnswerFrame,
    DevConversation,
    DevConversationTombstone,
    DevFeedback,
    DevMessage,
    DevRun,
    DevRunNarrative,
    DevRunResolution,
    DevRunStageDiagnostic,
    DevToolCall,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._chaos_3292_preflight import versions
from tests._helpers import tables_of

_TABLES = tables_of(
    User,
    Organization,
    DevConversation,
    DevMessage,
    DevRun,
    DevToolCall,
    DevFeedback,
    DevConversationTombstone,
    DevAnswerFrame,
    DevRunNarrative,
    DevRunResolution,
    DevRunStageDiagnostic,
)
_DIGEST = "sha256:" + ("a" * 64)


@pytest_asyncio.fixture
async def seeded(tmp_path: Path):
    database = tmp_path / "chaos-3441-record-frame-savepoint.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="ask-dev", name="Ask Dev"),
                User(id=user_id, email="ask-dev@example.com"),
            ]
        )
        await session.commit()
    try:
        yield engine, maker, org_id, user_id
    finally:
        await engine.dispose()


def _validated_answer(
    conversation_id: uuid.UUID, answer_id: uuid.UUID
) -> dict[str, Any]:
    return {
        "schema_version": "dev_answer.v1",
        "answer_id": str(answer_id),
        "conversation_id": str(conversation_id),
        "summary": "The evidence-backed answer can be rendered from storage.",
        "claims": [],
        "metrics": [],
        "evidence": [],
    }


def _identity_validator(payload: Any) -> Any:
    return payload


async def _seed_run(
    maker: async_sessionmaker[AsyncSession],
    org_id: uuid.UUID,
    user_id: uuid.UUID,
) -> tuple[uuid.UUID, uuid.UUID]:
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="How is Nightfall doing?",
            scope_snapshot={},
        )
        await session.commit()
        return conversation.id, accepted.run.id


_FRAME_INSERT_FAULT = """
CREATE TRIGGER chaos_3441_frame_insert_fault
BEFORE INSERT ON dev_answer_frames
BEGIN
    SELECT RAISE(ABORT, 'simulated storage failure on the frame insert');
END
"""


async def _arm_frame_insert_fault(session: AsyncSession) -> None:
    """Make the database itself reject the frame INSERT, and nothing else.

    A trigger is used rather than a listener that raises before the driver
    runs (which proves only SQLAlchemy's plumbing -- Codex adversarial
    review round 1) and rather than a blanket ``PRAGMA query_only`` (which
    also fails unrelated statements, including the savepoint-entry flush,
    so the test would stop being about the frame write at all).
    """

    await session.execute(text(_FRAME_INSERT_FAULT))


async def _disarm_frame_insert_fault(session: AsyncSession) -> None:
    await session.execute(text("DROP TRIGGER chaos_3441_frame_insert_fault"))


def _error_frame(run_id: uuid.UUID):
    return dev_terminal_frames.build_error_frame(
        code="scope_not_found",
        run_id=str(run_id),
        generated_at=datetime.now(UTC),
        versions=versions(),
    )


@pytest.mark.asyncio
async def test_chaos_3441_mid_flush_frame_fault_keeps_the_real_answer_row(
    seeded,
) -> None:
    """The real-answer half of the window CHAOS-3441 closes.

    The fault is a genuine database-side rejection of the frame INSERT: a
    trigger raises ABORT, so the error comes back through the driver from
    the statement executing inside the savepoint, not from a listener that
    short-circuits before the driver ever sees it. That distinction is the
    point -- an earlier version of this test raised a hand-built
    ``OperationalError`` from ``before_cursor_execute`` and so proved only
    SQLAlchemy's exception plumbing (Codex adversarial review round 1,
    HIGH). It is also targeted at that one statement, so the proof cannot
    quietly become about some other write failing.

    What must hold: the failure unwinds to ``record_frame``'s SAVEPOINT
    only. The ``DevAnswer`` transcript row flushed just before it survives,
    and the outer session stays usable for ``terminal()``'s own
    ``update_run`` write with no ``session.rollback()`` in between -- which
    is exactly what ``orchestrator.finish()`` does on this path (it
    deliberately does NOT roll back once a transcript row is flushed).

    What this does NOT claim, since no savepoint could deliver it: survival
    of a physical connection loss. If the connection dies there is no
    session left to roll back to a savepoint and an uncommitted row is gone
    by definition; that case is recovered only by
    ``force_terminal_fallback``'s fresh session.
    """

    _engine, maker, org_id, user_id = seeded
    conversation_id, run_id = await _seed_run(maker, org_id, user_id)
    answer_id = uuid.uuid4()

    async with maker() as session:
        service = DevPersistenceService(session)
        await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            answer_payload=_validated_answer(conversation_id, answer_id),
            validator=_identity_validator,
            scope_snapshot={},
        )

        frame = _error_frame(run_id)
        # The database, not the test harness, refuses the write.
        await _arm_frame_insert_fault(session)
        with pytest.raises(IntegrityError) as raised:
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(frame.frame_id),
                public_outcome=frame.public_outcome.value,
                payload=frame.model_dump(mode="json"),
            )
        # Proof the failure came from the driver executing the INSERT, so
        # this test cannot silently degrade into asserting on a fault the
        # harness itself raised.
        assert isinstance(raised.value.orig, sqlite3.IntegrityError)
        assert "simulated storage failure on the frame insert" in str(raised.value.orig)
        await _disarm_frame_insert_fault(session)

        # The outer session is NOT rollback-only: terminal()'s write lands
        # without a rollback() call and without PendingRollbackError.
        run = await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="completed",
            answer_id=answer_id,
            provider_source="platform",
            provider_fingerprint=_DIGEST,
            model_fingerprint=_DIGEST,
            prompt_version="dev-system.v1",
            tool_contract_version="dev-tools.v1",
            metric_version="metric-registry.v1",
            query_version="dev-query.v1",
        )
        assert run is not None
        await session.commit()

    async with maker() as reader:
        rows = (
            await reader.scalars(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
        ).all()
        assert len(rows) == 1
        assert rows[0].answer_id == answer_id
        run_row = await reader.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.state == "completed"
        assert (
            await reader.scalar(
                select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
            )
        ) is None


@pytest.mark.asyncio
async def test_chaos_3441_failed_frame_write_never_leaks_a_v2_contract_tag(
    seeded,
) -> None:
    """The ORM half of "rolls back only the frame write" -- an invariant
    lock, honestly labelled: the suspected leak it was written to catch does
    not exist (see this module's docstring).

    ``record_frame`` tags its owned run (``contract_generation = 'v2'``,
    ``public_outcome``) inside the SAVEPOINT, as in-Python mutations on a
    persistent object. A savepoint rollback restores the database, and
    SQLAlchemy additionally expires what the failed flush touched, so the
    next flush on the outer session does not re-emit those tags: no run is
    committed as v2, with a ``public_outcome``, and no frame row -- the
    inconsistency migration 0074's downgrade guard exists to detect. This
    test pins that end state so a future rework of the savepoint (or of the
    flush-error handling it relies on) cannot reintroduce it silently; it
    fails today if ``record_frame``'s ``begin_nested`` is removed.
    """

    _engine, maker, org_id, user_id = seeded
    _conversation_id, run_id = await _seed_run(maker, org_id, user_id)
    frame = _error_frame(run_id)

    async with maker() as session:
        service = DevPersistenceService(session)

        # A conflicting frame row for the SAME run, written directly (never
        # through record_frame, which would tag the run itself) so the
        # failing call below is the FIRST one to touch those tags. The
        # payload is a real, contract-valid frame: the session's own
        # flush-time payload guard rejects anything less.
        squatter = dev_terminal_frames.build_error_frame(
            code="internal_error",
            run_id=str(run_id),
            generated_at=datetime.now(UTC),
            versions=versions(),
        )
        session.add(
            DevAnswerFrame(
                run_id=run_id,
                org_id=org_id,
                user_id=user_id,
                frame_id=uuid.UUID(squatter.frame_id),
                public_outcome=squatter.public_outcome.value,
                payload=squatter.model_dump(mode="json"),
            )
        )
        await session.flush()

        with pytest.raises(IntegrityError):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(frame.frame_id),
                public_outcome=frame.public_outcome.value,
                payload=frame.model_dump(mode="json"),
            )

        run = await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="failed",
            safe_error_code="scope_not_found",
        )
        assert run is not None
        await session.commit()

    async with maker() as reader:
        run_row = await reader.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.state == "failed"
        # The frame write failed, so its run tags must never have landed.
        assert run_row.contract_generation == "v1"
        assert run_row.public_outcome is None


@pytest.mark.asyncio
async def test_chaos_3441_savepoint_opens_before_the_pre_write_selects(
    seeded,
) -> None:
    """Codex adversarial review round 1 (HIGH, confirmed): the savepoint has
    to enclose ``record_frame``'s and ``record_narrative``'s PRE-write
    SELECTs, not only their flush.

    Why the boundary itself is the assertion, rather than a fault-injection:
    the failure this closes is a server-side statement failure -- a
    PostgreSQL ``statement_timeout`` firing on the ownership or
    authorization SELECT -- which aborts the WHOLE transaction, so every
    later statement fails with ``InFailedSqlTransaction`` and the caller's
    already-flushed transcript row dies with the commit that can no longer
    happen. ``ROLLBACK TO SAVEPOINT`` is what makes an aborted PostgreSQL
    transaction usable again, and it can only do that if the SAVEPOINT was
    emitted BEFORE the statement that failed. sqlite does not abort a
    transaction on a failed statement, so a fault-injected test here would
    pass whether the savepoint enclosed those SELECTs or not -- a test that
    cannot fail. The statement ORDER is the part sqlite can observe
    faithfully, and it is exactly the property the fix establishes: this
    test fails on the pre-fix code, where the SELECTs ran first.
    """

    engine, maker, org_id, user_id = seeded
    _conversation_id, run_id = await _seed_run(maker, org_id, user_id)
    frame = _error_frame(run_id)

    captured: list[str] = []
    recording = {"on": False}

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def _capture(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if recording["on"]:
            captured.append(" ".join(statement.split()))

    def _first(predicate) -> int:
        for index, statement in enumerate(captured):
            if predicate(statement):
                return index
        raise AssertionError(f"no statement matched in the captured window: {captured}")

    async with maker() as session:
        service = DevPersistenceService(session)

        recording["on"] = True
        await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame.frame_id),
            public_outcome=frame.public_outcome.value,
            payload=frame.model_dump(mode="json"),
        )
        recording["on"] = False

        savepoint = _first(lambda s: s.startswith("SAVEPOINT"))
        ownership_select = _first(lambda s: "FROM dev_runs" in s)
        frame_insert = _first(lambda s: s.startswith("INSERT INTO dev_answer_frames"))
        assert savepoint < ownership_select < frame_insert, captured

        # record_narrative's own pre-write SELECTs (run ownership + the
        # run's recorded frame) must sit inside its savepoint too. A
        # rejected narrative is enough to exercise them: the SELECTs run
        # before the payload cross-checks that reject it.
        captured.clear()
        recording["on"] = True
        with pytest.raises(DevPersistenceValidationError):
            await service.record_narrative(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                narrative_id=uuid.uuid4(),
                frame_id=uuid.uuid4(),  # not the run's recorded frame
                mode="deterministic_fallback",
                provider_fingerprint=None,
                narrative_text="rejected before any write",
                payload={},
            )
        recording["on"] = False

        narrative_savepoint = _first(lambda s: s.startswith("SAVEPOINT"))
        narrative_select = _first(lambda s: "FROM dev_runs" in s)
        frame_lookup = _first(lambda s: "FROM dev_answer_frames" in s)
        assert narrative_savepoint < narrative_select, captured
        assert narrative_savepoint < frame_lookup, captured


@pytest.mark.asyncio
async def test_chaos_3441_frame_failure_keeps_the_runs_flushed_diagnostics(
    seeded,
) -> None:
    """The ticket's own words: a mid-flush frame failure must leave the outer
    session usable "for the transcript row + diagnostics".

    The transcript row is covered above; this covers the diagnostics. A
    stage diagnostic and a tool call are flushed on the outer session (as
    ``PersistenceRunRecorder`` does throughout a run), then the frame write
    fails at the driver. Both forensic rows must still commit -- they are
    the only evidence an operator has for a run that failed here, and
    nothing re-persists them if they are lost.
    """

    _engine, maker, org_id, user_id = seeded
    _conversation_id, run_id = await _seed_run(maker, org_id, user_id)
    frame = _error_frame(run_id)

    async with maker() as session:
        service = DevPersistenceService(session)
        await service.append_stage_diagnostic(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            ordinal=1,
            stage_id="resolving_subjects",
            status="completed",
            latency_ms=12,
            counts={"candidates": 3},
        )
        await service.append_tool_call(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            ordinal=1,
            tool_id="team_workload",
            tool_version="dev-tools.v1",
            canonical_input_hash=_DIGEST,
            safe_scope_summary={"teams": 1},
            status="completed",
            latency_ms=34,
            row_count=7,
        )

        await _arm_frame_insert_fault(session)
        with pytest.raises(IntegrityError):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(frame.frame_id),
                public_outcome=frame.public_outcome.value,
                payload=frame.model_dump(mode="json"),
            )
        await _disarm_frame_insert_fault(session)

        run = await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="failed",
            safe_error_code="internal_error",
        )
        assert run is not None
        await session.commit()

    async with maker() as reader:
        diagnostics = (
            await reader.scalars(
                select(DevRunStageDiagnostic).where(
                    DevRunStageDiagnostic.run_id == run_id
                )
            )
        ).all()
        tool_calls = (
            await reader.scalars(
                select(DevToolCall).where(DevToolCall.run_id == run_id)
            )
        ).all()
        assert [d.stage_id for d in diagnostics] == ["resolving_subjects"]
        assert [c.tool_id for c in tool_calls] == ["team_workload"]


@pytest.mark.asyncio
async def test_chaos_3441_transcript_writes_leave_no_unflushed_state(
    seeded,
) -> None:
    """A savepoint cannot protect a write that is flushed outside it, and
    that is what unflushed state guarantees.

    ``SessionTransaction._take_snapshot()`` flushes the session's pending
    state BEFORE emitting the SAVEPOINT (sqlalchemy/orm/session.py), so any
    dirty attribute a persistence method leaves behind is emitted by the
    NEXT operation's savepoint entry -- outside every savepoint. The
    transcript writes all touch their conversation (``updated_at``, and the
    30-day ``expires_at``), and that mutation used to be made after their
    savepoint block: the resulting ``UPDATE dev_conversations`` was carried
    into ``record_frame``'s savepoint-entry flush, where a server-side
    failure on it poisons the session and destroys the transcript row that
    had just been flushed -- this ticket's exact loss, via a statement
    nobody was looking at. Found while building the fault injection for the
    test above, which failed for precisely this reason.

    Two assertions, because either alone is weak: the conversation UPDATE
    lands INSIDE the savepoint (statement order), and the method returns
    with nothing dirty left for someone else's savepoint entry to flush.
    """

    engine, maker, org_id, user_id = seeded
    conversation_id, _run_id = await _seed_run(maker, org_id, user_id)
    answer_id = uuid.uuid4()

    captured: list[str] = []
    recording = {"on": False}

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def _capture(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if recording["on"]:
            captured.append(" ".join(statement.split()))

    async with maker() as session:
        service = DevPersistenceService(session)
        recording["on"] = True
        await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            answer_payload=_validated_answer(conversation_id, answer_id),
            validator=_identity_validator,
            scope_snapshot={},
        )
        recording["on"] = False

        savepoint = next(i for i, s in enumerate(captured) if s.startswith("SAVEPOINT"))
        release = next(
            i for i, s in enumerate(captured) if s.startswith("RELEASE SAVEPOINT")
        )
        conversation_update = next(
            i
            for i, s in enumerate(captured)
            if s.startswith("UPDATE dev_conversations")
        )
        assert savepoint < conversation_update < release, captured

        # Nothing is left for the next savepoint entry to flush outside its
        # own savepoint.
        assert not session.dirty, session.dirty
        assert not session.new, session.new
