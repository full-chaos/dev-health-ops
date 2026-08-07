"""RED-first coverage for CHAOS-3544: 0-day conversations that never complete
a run are retained forever.

An organisation on the 0-day retention tier has chosen immediate deletion.
Two reachable shapes are retained indefinitely instead:

* **(a) zero runs** -- ``POST /conversations`` is standalone
  (``router.py``), so a conversation exists the moment it is created and no
  message, and therefore no ``DevRun``, is required. Opened and abandoned
  without asking anything is ordinary use of an assistant panel, not an edge
  case.
* **(b) a run that never terminates** -- crash, cancellation, or an orphaned
  non-terminal run.

Why nothing collects either today:

* ``create_conversation`` stamps ``expires_at = NULL`` for ``retention_days
  == 0``; only the 30-day tier gets a creation stamp.
* ``_stamp_ephemeral_expiry_if_terminal`` is the only 0-day stamp, and it
  fires exclusively on a run reaching terminal.
* ``cleanup_expired`` selects on ``expires_at IS NOT NULL``.
* ``backfill_stranded_ephemeral_expiry`` requires ``has_a_run AND
  ~has_a_non_terminal_run`` -- excluding (a) by the first condition and (b)
  by the second.
* ``recover_stale_non_terminal_run`` cannot rescue (b): its own docstring
  says it fires "from the replay path itself, at the moment a caller
  actually asks for this run again, rather than a background sweep". A
  crashed run nobody re-requests is never recovered.

THE FIX, AND THE TRAP IT HAS TO AVOID. The obvious remedy -- stamp
``expires_at`` at creation, the one guaranteed event -- cannot be taken
literally. ``cleanup_expired`` has NO in-flight protection whatsoever; it is
safe today only because a 0-day row is never stamped before its run is
terminal. Stamping ``now`` at creation would make every 0-day conversation
deletable the instant it exists, purging conversations while the user is
still typing their first message. That is strictly worse than the defect.

So the creation stamp is GRACED, and the grace is derived rather than
guessed: ``DevRunLimits.wall_seconds`` is 45 seconds and
``_STALE_NON_TERMINAL_RUN_THRESHOLD`` is 5 minutes, the latter documented as
"comfortably longer than any run that is still genuinely in flight could
take without something else already having failed it". An hour is 80x the
first and 12x the second. A conversation that DOES complete a turn is still
stamped to ``now`` at terminal and purged immediately, exactly as before --
the grace only ever affects conversations that never complete one, which is
precisely the stranded population.

``test_an_active_conversation_is_not_purged_while_in_flight`` is the test
that kills the literal stamp-at-creation. It was watched failing against
that implementation before this one was written.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.persistence import (
    DevPersistenceNotFound,
    DevPersistenceService,
)
from dev_health_ops.models.dev_persistence import DevConversation
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of


@dataclass
class Clock:
    """A controllable ``now`` -- the grace is an hour, and no test sleeps."""

    value: datetime

    def __call__(self) -> datetime:
        return self.value


@pytest_asyncio.fixture
async def retention(tmp_path: Path):
    database = tmp_path / "chaos-3544-retention.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    from dev_health_ops.models.dev_persistence import (
        DevAnswerFrame,
        DevConversationTombstone,
        DevFeedback,
        DevMessage,
        DevRun,
        DevRunIntent,
        DevRunNarrative,
        DevRunQuaShadow,
        DevRunResolution,
        DevRunSourceObservation,
        DevRunStageDiagnostic,
        DevRunSubjectSet,
        DevToolCall,
    )

    tables = tables_of(
        User,
        Organization,
        DevConversation,
        DevMessage,
        DevRun,
        DevToolCall,
        DevFeedback,
        DevConversationTombstone,
        DevAnswerFrame,
        DevRunResolution,
        DevRunNarrative,
        DevRunSubjectSet,
        DevRunIntent,
        DevRunSourceObservation,
        DevRunStageDiagnostic,
        DevRunQuaShadow,
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=tables
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
        yield maker, org_id, user_id
    finally:
        await engine.dispose()


_START = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)


async def _exists(maker, conversation_id: uuid.UUID) -> bool:
    async with maker() as session:
        return await session.get(DevConversation, conversation_id) is not None


async def _sweep(maker, clock: Clock) -> int:
    """One real ``cleanup_expired`` tick, committed, as the beat task runs it."""

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        result = await service.cleanup_expired(limit=100)
        await session.commit()
    return result.purged


@pytest.mark.asyncio
async def test_an_abandoned_zero_run_conversation_is_eventually_purged(
    retention,
) -> None:
    """Stranded shape (a): created and abandoned before any message.

    The org selected immediate deletion; today this row is retained forever,
    because every purge path keys off an ``expires_at`` that nothing will
    ever set for it.
    """

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        await session.commit()

    clock.value = _START + timedelta(days=1)
    purged = await _sweep(maker, clock)

    assert purged == 1, (
        "CHAOS-3544: a 0-day conversation abandoned before any message must "
        "eventually be purged. The organisation chose immediate deletion; "
        "retaining it forever is a retention-policy violation in the tier "
        "whose entire promise is deletion."
    )
    assert not await _exists(maker, conversation.id)


@pytest.mark.asyncio
async def test_a_conversation_whose_run_never_terminates_is_eventually_purged(
    retention,
) -> None:
    """Stranded shape (b): a run left non-terminal by a crash or cancellation.

    ``recover_stale_non_terminal_run`` cannot save this one -- it fires only
    from the replay path, when a caller asks for the run again. A run nobody
    re-requests is never recovered, so the conversation is retained forever.
    """

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="This run never reaches a terminal state.",
            scope_snapshot={},
        )
        await session.commit()

    clock.value = _START + timedelta(days=1)
    purged = await _sweep(maker, clock)

    assert purged == 1, (
        "CHAOS-3544: a 0-day conversation whose run never terminated must "
        "eventually be purged -- no existing path can rescue it, so today it "
        "is retained forever."
    )
    assert not await _exists(maker, conversation.id)


@pytest.mark.asyncio
async def test_an_active_conversation_is_not_purged_while_in_flight(
    retention,
) -> None:
    """THE guard against the obvious-but-wrong fix, and the reason the
    creation stamp is graced.

    ``cleanup_expired`` selects purely on ``expires_at <= now`` and has no
    in-flight protection at all. A literal ``expires_at = now`` at creation
    would make every 0-day conversation deletable the instant it exists --
    purging it while the user is still typing their first message, and
    deleting a live run's own conversation out from under it.

    That is strictly worse than the defect being fixed: it converts
    "retained forever" into "destroyed while in use". This test was watched
    FAILING against the literal implementation before the graced one was
    written.

    Asserted at two points inside the grace -- immediately, and just before
    it elapses -- so a grace accidentally set to zero or to a few seconds is
    caught rather than only a grace of exactly zero.
    """

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="A genuinely in-flight turn.",
            scope_snapshot={},
        )
        await session.commit()

    # Immediately: the run has not even started producing output yet.
    assert await _sweep(maker, clock) == 0, (
        "a 0-day conversation must NOT be purged the moment it is created -- "
        "cleanup_expired has no in-flight protection, so a literal "
        "stamp-at-creation deletes live conversations"
    )
    assert await _exists(maker, conversation.id)

    # And still safe near the end of the grace, which is an order of
    # magnitude beyond any run that could still be genuinely in flight.
    clock.value = _START + timedelta(minutes=55)
    assert await _sweep(maker, clock) == 0
    assert await _exists(maker, conversation.id)


@pytest.mark.asyncio
async def test_a_completed_conversation_is_still_purged_immediately(
    retention,
) -> None:
    """Preserved behaviour: the grace must not delay the case that already
    worked.

    A 0-day conversation whose run reaches terminal is stamped to ``now`` and
    collected on the very next sweep tick -- no grace, exactly as today. If
    this ever starts waiting an hour, the fix has quietly downgraded the
    promise for the common path instead of only the abandoned one.
    """

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="A turn that completes normally.",
            scope_snapshot={},
        )
        # Raw terminal mutation rather than update_run: update_run also
        # attempts the synchronous purge, and this test is about the
        # STAMP + sweep half, which is what must survive a failed or skipped
        # inline purge.
        accepted.run.state = "completed"
        await session.flush()
        stamped = await service._stamp_ephemeral_expiry_if_terminal(
            org_id=org_id, user_id=user_id, conversation_id=conversation.id
        )
        await session.commit()

    assert stamped is not None
    # No clock advance at all: the terminal stamp is `now`, not `now + grace`.
    assert await _sweep(maker, clock) == 1, (
        "a completed 0-day conversation must still be purged on the next "
        "tick with no grace -- the creation grace exists only for "
        "conversations that never complete a turn"
    )
    assert not await _exists(maker, conversation.id)


@pytest.mark.asyncio
async def test_a_30_day_conversation_is_untouched_by_any_of_this(
    retention,
) -> None:
    """The 30-day tier's semantics are correct and must not move.

    It already stamps at creation (``now + 30d``); nothing here changes that
    path, and a 30-day conversation must be unaffected by the ephemeral
    grace at every point the 0-day rows above are purged.
    """

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=30
        )
        await session.commit()

    # Well past the ephemeral grace, nowhere near 30 days.
    clock.value = _START + timedelta(days=2)
    assert await _sweep(maker, clock) == 0
    assert await _exists(maker, conversation.id)

    # And still collected on its own schedule.
    clock.value = _START + timedelta(days=31)
    assert await _sweep(maker, clock) == 1
    assert not await _exists(maker, conversation.id)


@pytest.mark.asyncio
async def test_the_creation_stamp_is_the_graced_one_not_now(retention) -> None:
    """Pins the stamp itself, not only its downstream effect.

    The sweep tests above would all pass for a grace of one second or one
    year. This asserts the actual persisted value against the constant, so a
    change to the grace is a deliberate edit to a named number rather than a
    silent drift nothing measures.
    """

    from dev_health_ops.api.dev.persistence.service import (
        EPHEMERAL_ABANDONED_GRACE,
    )

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        ephemeral = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        await session.commit()

    async with maker() as session:
        row = await session.get(DevConversation, ephemeral.id)
        assert row is not None
        assert row.expires_at is not None, (
            "a 0-day conversation must carry an expiry from the moment it "
            "exists -- creation is the one event guaranteed to happen"
        )
        stored = row.expires_at
        if stored.tzinfo is None:
            stored = stored.replace(tzinfo=UTC)
        assert stored == _START + EPHEMERAL_ABANDONED_GRACE


@pytest.mark.asyncio
async def test_the_grace_is_far_longer_than_any_run_can_live(retention) -> None:
    """The derivation, asserted rather than left in a comment.

    The grace is only safe because it is far beyond any run that could still
    be genuinely in flight. Both bounds are imported from where they are
    actually defined, so if either grows past the grace this fails instead of
    silently reintroducing the "purged while in use" failure mode.
    """

    from dev_health_ops.api.dev.orchestrator import DevRunLimits
    from dev_health_ops.api.dev.persistence.service import (
        EPHEMERAL_ABANDONED_GRACE,
    )
    from dev_health_ops.api.dev.router import _STALE_NON_TERMINAL_RUN_THRESHOLD

    assert EPHEMERAL_ABANDONED_GRACE >= 10 * _STALE_NON_TERMINAL_RUN_THRESHOLD, (
        "the grace must stay an order of magnitude above the threshold at "
        "which a non-terminal run is considered impossible-to-still-be-live"
    )
    assert (
        EPHEMERAL_ABANDONED_GRACE.total_seconds() >= 50 * DevRunLimits().wall_seconds
    ), "and far beyond a single run's own wall-clock limit"


@pytest.mark.asyncio
async def test_a_turn_started_late_in_the_grace_is_not_purged_under_it(
    retention,
) -> None:
    """Codex adversarial review, HIGH: the creation stamp is anchored to
    CREATION, but a turn can start at any point afterwards.

    Open an ephemeral conversation, leave it idle 55 minutes, then ask a
    question. The run is legitimately in flight -- but the expiry was set at
    T0 and comes due at T0+1h, and ``cleanup_expired`` has no run-state
    guard. The live run's own conversation is deleted out from under it,
    five minutes into a turn.

    This is the same purged-while-in-use failure the grace exists to
    prevent, arriving through a door the original in-flight test could not
    see: that test starts its run at creation time, so the whole grace is
    still ahead of it. Anchoring on creation was the mistake -- the stamp has
    to track ACTIVITY.
    """

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        await session.commit()

    # 55 minutes later the user finally asks something.
    clock.value = _START + timedelta(minutes=55)
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="Asked 55 minutes after opening the panel.",
            scope_snapshot={},
        )
        await session.commit()

    # Five minutes into that turn, the original creation-anchored expiry is
    # due. The run is still in flight.
    clock.value = _START + timedelta(minutes=60)
    assert await _sweep(maker, clock) == 0, (
        "a conversation whose turn started late in the grace must not be "
        "purged while that turn is in flight -- the expiry has to track "
        "activity, not creation"
    )
    assert await _exists(maker, conversation.id)


@pytest.mark.asyncio
async def test_a_resumed_pre_fix_conversation_is_not_stamped_and_purged(
    retention,
) -> None:
    """Codex adversarial review, HIGH: the backfill must not stamp a
    conversation somebody has just resumed.

    A row created before this fix still carries ``expires_at IS NULL`` and
    stays perfectly readable, so a user can come back to it after deploy and
    ask something. If the repair selects purely on how OLD the conversation
    is, it stamps that row to ``now`` for no reason but its age, and the same
    sweep tick purges it with a live run attached -- reintroducing precisely
    the in-flight protection the replaced run-state pair used to provide.

    The conversation here is days old and NULL-stamped: unambiguously a
    pre-fix stranded row by age alone. The only thing that makes it
    ineligible is that someone is using it right now.
    """

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        await session.commit()

    # Rewrite it into the genuine pre-fix shape: NULL expiry, days old.
    async with maker() as session:
        row = await session.get(DevConversation, conversation.id)
        assert row is not None
        row.expires_at = None
        row.created_at = _START - timedelta(days=3)
        row.updated_at = _START - timedelta(days=3)
        await session.commit()

    # The user resumes it and asks a question.
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="Resumed an old ephemeral conversation.",
            scope_snapshot={},
        )
        await session.commit()

    # A beat tick fires while that turn is in flight: stamp, then sweep.
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        await service.backfill_stranded_ephemeral_expiry(limit=100)
        await session.commit()
    purged = await _sweep(maker, clock)

    assert purged == 0, (
        "a resumed pre-fix conversation with a live run must not be stamped "
        "and purged for being old -- age alone is not idleness"
    )
    assert await _exists(maker, conversation.id)


@pytest.mark.asyncio
async def test_a_completed_conversation_cannot_be_written_to_again(
    retention,
) -> None:
    """Why a completed ephemeral conversation cannot be resurrected by a late
    write -- established by execution, and recorded because I nearly shipped
    a guard against it.

    ``_touch`` refreshes the ephemeral expiry on activity, so a touch landing
    AFTER ``_stamp_ephemeral_expiry_if_terminal`` would push a completed
    conversation an hour into the future, quietly delaying the immediate
    deletion 0-day promises. Measured directly against the helper: 12:00
    becomes 13:00.

    I added a parameter to stop that, then found the path is unreachable and
    removed it again. Two independent mechanisms already close it: ``finish()``
    writes the answer BEFORE the terminal transition, so the terminal stamp is
    last; and once stamped, the conversation is due, and every append path
    resolves through ``get_conversation``, which excludes expired rows. The
    append does not extend retention -- it fails closed.

    A guard I could not exercise through a reachable path would have been
    complexity that reads as protection. This test asserts the mechanism that
    actually does the work instead, so if either half ever changes, something
    fails.
    """

    maker, org_id, user_id = retention
    clock = Clock(_START)

    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}, retention_days=0
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="A turn that completes.",
            scope_snapshot={},
        )
        accepted.run.state = "completed"
        await session.flush()
        assert (
            await service._stamp_ephemeral_expiry_if_terminal(
                org_id=org_id, user_id=user_id, conversation_id=conversation.id
            )
            is not None
        )
        await session.commit()

    # A late assistant write cannot even find it -- so it cannot extend it.
    answer_id = uuid.uuid4()
    async with maker() as session:
        service = DevPersistenceService(session, now=clock)
        with pytest.raises(DevPersistenceNotFound):
            await service.append_assistant_answer(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation.id,
                answer_payload={
                    "schema_version": "dev_answer.v1",
                    "answer_id": str(answer_id),
                    "conversation_id": str(conversation.id),
                    "summary": "An answer written after the terminal stamp.",
                    "claims": [],
                    "metrics": [],
                    "evidence": [],
                },
                validator=lambda payload: payload,
                scope_snapshot={},
            )

    # And it is still due NOW, with no grace added.
    assert await _sweep(maker, clock) == 1
    assert not await _exists(maker, conversation.id)
