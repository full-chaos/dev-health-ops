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

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev import terminal_frames as dev_terminal_frames
from dev_health_ops.api.dev.persistence import DevPersistenceService
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

    A connection-level failure raised while the frame INSERT is being
    executed (the ticket's "dropped connection", not a constraint
    violation) must unwind to ``record_frame``'s SAVEPOINT only: the
    ``DevAnswer`` transcript row flushed just before it survives, and the
    outer session stays usable for ``terminal()``'s own ``update_run``
    write -- with no ``session.rollback()`` in between, which is what
    ``orchestrator.finish()`` actually does on this path (it deliberately
    does NOT roll back when a transcript row is already flushed).
    """

    engine, maker, org_id, user_id = seeded
    conversation_id, run_id = await _seed_run(maker, org_id, user_id)
    answer_id = uuid.uuid4()

    fault_armed = {"value": False}

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def _fail_frame_insert(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if fault_armed["value"] and "INSERT INTO dev_answer_frames" in statement:
            raise OperationalError(
                statement, parameters, Exception("simulated dropped connection")
            )

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
        fault_armed["value"] = True
        with pytest.raises(OperationalError):
            await service.record_frame(
                org_id=org_id,
                user_id=user_id,
                run_id=run_id,
                frame_id=uuid.UUID(frame.frame_id),
                public_outcome=frame.public_outcome.value,
                payload=frame.model_dump(mode="json"),
            )
        fault_armed["value"] = False

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
