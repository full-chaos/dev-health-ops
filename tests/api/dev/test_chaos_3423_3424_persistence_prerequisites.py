"""RED-first coverage for two Wave 4 Phase 2 prerequisites, found during the
2026-08-05 live fuzzy-diagnosis (CHAOS-3421) and required by the corpus
runner's case assertions / QUA shadow harness:

* CHAOS-3423 -- a clarification or error terminal (``answer is None`` in
  ``orchestrator.finish()``) persists a real ``dev_answer_frame.v1`` row
  (``record_frame``) but never a ``dev_messages`` assistant row, so the
  conversation transcript is structurally incomplete for exactly the turns
  where guidance matters most.
* CHAOS-3424 -- the entity-resolution ledger (``dev_run_resolutions``) is
  only ever appended on the preflight TERMINATE branch. A PROCEED decision
  that widens to organization scope for an unresolved bare name
  (``subject_preflight``'s ``proceeded_unresolved_bare_name``) leaves zero
  ledger rows -- the live incident's only evidence was a container log line.

Both suites drive the REAL ``DevOrchestrator.run()`` -> real
``PersistenceRunRecorder`` -> real ``DevPersistenceService`` -> a real (if
ephemeral) sqlite database, via ``tests._chaos_3292_preflight.
run_preflight_orchestrator``'s production-shaped wiring -- never the module's
own fake ``Recorder``, which only captures a call list and would prove
nothing about what actually lands in the tables the corpus runner reads.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev import router as dev_router_module
from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.persistence import (
    DevPersistenceNotFound,
    DevPersistenceService,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.llm.agent.contracts import AgentFinalAnswer
from dev_health_ops.llm.agent.scripted import ScriptedStep
from dev_health_ops.models.dev_persistence import (
    DevAnswerFrame,
    DevConversation,
    DevConversationTombstone,
    DevFeedback,
    DevMessage,
    DevRun,
    DevRunIntent,
    DevRunNarrative,
    DevRunResolution,
    DevRunSourceObservation,
    DevRunStageDiagnostic,
    DevRunSubjectSet,
    DevToolCall,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    Recorder,
    grounded_answer_payload,
    run_preflight_orchestrator,
    scope_dict,
    status_then_answer,
)
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
    DevRunResolution,
    DevRunNarrative,
    DevRunSubjectSet,
    DevRunIntent,
    DevRunSourceObservation,
    DevRunStageDiagnostic,
)

#: The exact real member of ``ScopeResolutionOutcome`` the CHAOS-3421 live
#: incident leaked -- reused verbatim from
#: ``test_chaos_3421_leak_graceful_terminal.py`` so this suite's PROCEED ->
#: no-answer-terminal scenario is the same shape as the live diagnosis, not
#: an invented one.
_LEAKED_TOKEN = "forbidden_or_not_found"


def _leaking_but_not_narrating(org_id: str):
    """A scripted final answer that echoes the raw resolve_scope.v1 outcome
    token without narrating the bare name -- drives the run to the
    leak-scan's graceful ``scope_not_found`` reroute (CHAOS-3421), which is
    a no-answer terminal reached from a PROCEED preflight decision rather
    than a preflight TERMINATE.

    Returns a script factory (not the script itself): the claim's own
    ``validity_scope`` must carry THIS run's own ``org_id`` -- this suite's
    tests use a fresh UUID per run (real persistence needs one), unlike the
    shared harness's fixed ``ORG_ID`` constant, so ``scope_dict()``'s
    fixture default would otherwise fail grounding validation for a reason
    unrelated to what is under test (an org mismatch), never reaching the
    leak scan at all.
    """

    def factory(script_id: str) -> list[ScriptedStep]:
        steps = status_then_answer(script_id)
        steps[-1] = ScriptedStep(
            decision=AgentFinalAnswer(
                grounded_answer_payload(
                    script_id=script_id,
                    summary=(
                        f"The scope resolution tool returned {_LEAKED_TOKEN} "
                        "for the requested subject."
                    ),
                    validity_scope=scope_dict(organization_id=org_id),
                )
            )
        )
        return steps

    return factory


@pytest_asyncio.fixture
async def seeded(tmp_path: Path):
    database = tmp_path / "chaos-3423-3424-persistence.db"
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
        yield maker, org_id, user_id
    finally:
        await engine.dispose()


async def _seed_run(
    maker: async_sessionmaker[AsyncSession],
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    *,
    question: str,
) -> tuple[uuid.UUID, uuid.UUID]:
    """Seed a real conversation + accepted run the way ``router.create_message``
    does, ahead of the orchestrator call -- so the ``recorder_factory`` below
    can build a REAL ``PersistenceRunRecorder`` bound to a row that already
    exists, exactly like production.
    """

    seed_scope = scope_dict(organization_id=str(org_id))
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope=seed_scope
        )
        accepted = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question=question,
            scope_snapshot=seed_scope,
        )
        await session.commit()
        return conversation.id, accepted.run.id


@pytest.mark.asyncio
async def test_chaos_3423_clarification_terminal_persists_an_assistant_transcript_row(
    seeded,
) -> None:
    """A needs_clarification preflight TERMINATE already persists a real
    ``dev_answer_frame.v1`` (``record_frame``) -- but until CHAOS-3423 is
    fixed, ``dev_messages`` gets no matching assistant row, so a transcript
    read for this conversation renders the question with no answer bubble.
    """

    maker, org_id, user_id = seeded
    question = "What's the status of the Atlas project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)

        def recorder_factory() -> Recorder:
            # Statically typed as the shared harness's own `Recorder` (what
            # `run_preflight_orchestrator`'s `recorder_factory` parameter
            # declares) but actually a REAL `PersistenceRunRecorder` at
            # runtime -- both satisfy the same `RunRecorder` protocol
            # structurally, so `DevOrchestrator` neither knows nor cares.
            # This cast only narrows the STATIC type back to the shared
            # fixture's declared shape without widening it (and every other
            # existing caller's `output.recorder.<fake-only-attribute>`
            # access) for the whole test suite; it changes no runtime
            # behavior.
            return cast(
                Recorder,
                PersistenceRunRecorder(
                    service,
                    org_id=org_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    run_id=run_id,
                    provider_source="platform",
                ),
            )

        output = await run_preflight_orchestrator(
            question=question,
            entities=[
                (str(org_id), ATLAS_PROJECT_ONE),
                (str(org_id), ATLAS_PROJECT_TWO),
            ],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="chaos-3423-clarification",
            recorder_factory=recorder_factory,
        )
        await session.commit()

        # Setup control: this really is the needs_clarification preflight
        # TERMINATE, not some other no-answer shape.
        assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
        assert output.result.answer is None
        assert output.result.error is not None
        assert output.result.error.code == "scope_ambiguous"

        assistant_rows = (
            await session.scalars(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
        ).all()
        assert len(assistant_rows) == 1, (
            "CHAOS-3423: a needs_clarification terminal must persist exactly "
            "one dev_messages assistant row so the transcript renders an "
            "answer bubble on reload -- got "
            f"{len(assistant_rows)}."
        )
        row = assistant_rows[0]
        assert row.content == output.result.error.safe_message
        assert row.answer_id is not None
        assert row.answer_payload is not None


@pytest.mark.asyncio
async def test_chaos_3424_widening_proceed_persists_the_resolution_ledger(
    seeded,
) -> None:
    """The live CHAOS-3421 diagnosis shape: an unresolved bare name widens
    to organization scope (PROCEED, diagnostic
    ``proceeded_unresolved_bare_name``), the model then leaks the raw
    ``resolve_scope.v1`` outcome token, and the run reroutes to a graceful
    no-answer terminal (``scope_not_found``). Until CHAOS-3424 is fixed,
    ``dev_run_resolutions`` gets zero rows for this run even though the
    preflight built a real ledger entry for the unresolved mention -- this
    run also doubles as CHAOS-3423 coverage for a non-clarification
    no-answer terminal (the leak-scrub reroute never goes through the
    preflight TERMINATE branch at all).
    """

    maker, org_id, user_id = seeded
    question = "How is Nightfall doing?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)

        def recorder_factory() -> Recorder:
            # Statically typed as the shared harness's own `Recorder` (what
            # `run_preflight_orchestrator`'s `recorder_factory` parameter
            # declares) but actually a REAL `PersistenceRunRecorder` at
            # runtime -- both satisfy the same `RunRecorder` protocol
            # structurally, so `DevOrchestrator` neither knows nor cares.
            # This cast only narrows the STATIC type back to the shared
            # fixture's declared shape without widening it (and every other
            # existing caller's `output.recorder.<fake-only-attribute>`
            # access) for the whole test suite; it changes no runtime
            # behavior.
            return cast(
                Recorder,
                PersistenceRunRecorder(
                    service,
                    org_id=org_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    run_id=run_id,
                    provider_source="platform",
                ),
            )

        output = await run_preflight_orchestrator(
            question=question,
            entities=[(str(org_id), ASK_DEV_PROJECT)],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script=_leaking_but_not_narrating(str(org_id)),
            script_id="chaos-3424-widening",
            recorder_factory=recorder_factory,
        )
        await session.commit()

        # Setup control: the run really did take the unresolved-bare-name
        # organization-wide PROCEED branch, and really did reroute to the
        # graceful no-answer terminal rather than terminating in preflight.
        run_row = await session.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.preflight_outcome == "proceeded_unresolved_bare_name"
        assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
        assert output.result.answer is None
        assert output.result.error is not None
        assert output.result.error.code == "scope_not_found"
        assert _LEAKED_TOKEN not in output.result.error.safe_message

        ledger_rows = (
            await session.scalars(
                select(DevRunResolution).where(DevRunResolution.run_id == run_id)
            )
        ).all()
        assert len(ledger_rows) >= 1, (
            "CHAOS-3424: a widening PROCEED decision must persist its "
            "resolution ledger entries so the incident is auditable from "
            f"data, not only a log line -- got {len(ledger_rows)} rows."
        )
        assert all(row.outcome != "exact_match" for row in ledger_rows), (
            "the unresolved bare name's own entry must be one of the "
            "unresolved outcomes, never fabricated as a match"
        )

        # CHAOS-3423 generalizes to this non-clarification no-answer
        # terminal too.
        assistant_rows = (
            await session.scalars(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
        ).all()
        assert len(assistant_rows) == 1
        assert assistant_rows[0].content == output.result.error.safe_message


@pytest.mark.asyncio
async def test_completed_answer_still_persists_exactly_one_assistant_row(
    seeded,
) -> None:
    """Regression guard: an ordinary completed answer must keep persisting
    exactly one ``dev_messages`` assistant row through ``record_answer`` --
    the CHAOS-3423 fix must never double-write when a real ``DevAnswer``
    already exists.
    """

    maker, org_id, user_id = seeded
    question = "What's the status of the Ask Dev project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)

        def recorder_factory() -> Recorder:
            # Statically typed as the shared harness's own `Recorder` (what
            # `run_preflight_orchestrator`'s `recorder_factory` parameter
            # declares) but actually a REAL `PersistenceRunRecorder` at
            # runtime -- both satisfy the same `RunRecorder` protocol
            # structurally, so `DevOrchestrator` neither knows nor cares.
            # This cast only narrows the STATIC type back to the shared
            # fixture's declared shape without widening it (and every other
            # existing caller's `output.recorder.<fake-only-attribute>`
            # access) for the whole test suite; it changes no runtime
            # behavior.
            return cast(
                Recorder,
                PersistenceRunRecorder(
                    service,
                    org_id=org_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    run_id=run_id,
                    provider_source="platform",
                ),
            )

        output = await run_preflight_orchestrator(
            question=question,
            entities=[(str(org_id), ASK_DEV_PROJECT)],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="chaos-3423-regression-answer",
            recorder_factory=recorder_factory,
        )
        await session.commit()

        assert output.result.state is RunState.COMPLETED
        assert output.result.answer is not None
        assert output.result.error is None

        assistant_rows = (
            await session.scalars(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
        ).all()
        assert len(assistant_rows) == 1
        assert assistant_rows[0].answer_payload["schema_version"] in {
            "dev_answer.v1",
            "dev_answer.v2",
        }


@pytest.mark.asyncio
async def test_chaos_3423_new_row_shape_is_readable_by_prompt_history_and_transcript(
    seeded,
) -> None:
    """Both existing readers of ``dev_messages.answer_payload`` unconditionally
    called ``DevAnswer.model_validate`` on every assistant row before
    CHAOS-3423 -- a ``dev_error.v1``-shaped row would crash both. They now
    diverge deliberately (Codex adversarial review round 2, confirmed with
    direct evidence from the sibling ``dev-health-web`` checkout):

    * ``router._bounded_prompt_history`` (internal-only -- feeds the next
      turn's model prompt, never reaches a client) parses the row and
      includes its ``safe_message``, so the model keeps continuity with a
      prior clarification/error turn.
    * ``router.get_conversation_transcript`` (client-facing wire response)
      OMITS the row instead -- the checked-in web client's own
      ``AskDevProvider.toTranscriptEntry`` throws for any assistant entry
      without ``answer``, and the wire contract deliberately gained no new
      field here, so an old or new client sees byte-identical transcript
      responses either way (the turn simply has no answer bubble, exactly
      the pre-CHAOS-3423 behavior) until a coordinated client update ships.
    """

    maker, org_id, user_id = seeded
    question = "What's the status of the Atlas project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)

        def recorder_factory() -> Recorder:
            # Statically typed as the shared harness's own `Recorder` (what
            # `run_preflight_orchestrator`'s `recorder_factory` parameter
            # declares) but actually a REAL `PersistenceRunRecorder` at
            # runtime -- both satisfy the same `RunRecorder` protocol
            # structurally, so `DevOrchestrator` neither knows nor cares.
            # This cast only narrows the STATIC type back to the shared
            # fixture's declared shape without widening it (and every other
            # existing caller's `output.recorder.<fake-only-attribute>`
            # access) for the whole test suite; it changes no runtime
            # behavior.
            return cast(
                Recorder,
                PersistenceRunRecorder(
                    service,
                    org_id=org_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    run_id=run_id,
                    provider_source="platform",
                ),
            )

        output = await run_preflight_orchestrator(
            question=question,
            entities=[
                (str(org_id), ATLAS_PROJECT_ONE),
                (str(org_id), ATLAS_PROJECT_TWO),
            ],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="chaos-3423-readers",
            recorder_factory=recorder_factory,
        )
        await session.commit()
        assert output.result.error is not None

        history = await service.list_prompt_history_messages(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            exclude_message_id=uuid.uuid4(),
            limit=10,
        )
        turns = dev_router_module._bounded_prompt_history(history)
        assert [t.role for t in turns] == ["user", "assistant"]
        assert turns[1].content == output.result.error.safe_message

        user = AuthenticatedUser(
            user_id=str(user_id),
            email="ask-dev@example.com",
            org_id=str(org_id),
            role="member",
        )
        transcript = await dev_router_module.get_conversation_transcript(
            conversation_id,
            (user, service, "request-chaos-3423"),
        )
        # CHAOS-3423 Codex round 2 pivot: the client-facing transcript
        # endpoint omits the no-answer row entirely (never crashes, never
        # exposes a new field the current web client cannot handle) -- the
        # transcript looks exactly like it did before this change shipped.
        assert [item.role for item in transcript.items] == ["user"]


@pytest.mark.asyncio
async def test_chaos_3423_frame_write_failure_does_not_discard_the_transcript_row(
    seeded, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codex adversarial review (HIGH, confirmed): the no-answer transcript
    row is flushed on the SAME uncommitted session as the compatibility
    frame built right after it. If ``record_frame`` then fails,
    ``finish()``'s existing failure handler used to unconditionally decide
    "no prior write to protect" from `answer is None` alone -- which
    stopped being true the moment ``record_error_message`` started writing
    into that exact branch, so a plain ``session.rollback()`` there would
    silently discard the just-written transcript row along with the failed
    frame. This drives that exact failure through a REAL
    ``DevPersistenceService.record_frame`` failure (not a recorder fake) and
    asserts the row survives.

    Scope, stated honestly (Codex adversarial review round 2): this
    ``record_frame`` failure is raised BEFORE any real ``session.flush()``
    runs, so it proves the pre-flush/construction-failure case (e.g.
    ``UnregisteredTerminalCode``, an unrelated Python bug in frame
    construction) -- never a genuine mid-flush database error that marks
    the session rollback-only. That narrower case is a PRE-EXISTING,
    documented residual (``DevPersistenceService.force_terminal_fallback``'s
    own docstring, CHAOS-3297 round 3 Finding 2): a real ``record_frame``
    flush failure can already leave ``terminal()`` raising
    ``PendingRollbackError`` for a genuine ``DevAnswer`` today (that write
    is ALSO left un-rolled-back, for the identical "don't discard a
    successful write" reason), recovered only at the system level (the run
    still reaches a coherent terminal state via ``force_terminal_fallback``
    on a fresh session -- the transcript row can rarely be lost in that
    narrow correlated-failure window, exactly as a real answer already
    could be). This change makes the no-answer path symmetric with that
    existing, accepted tradeoff -- not worse than it -- rather than closing
    it for either path; closing it fully would mean SAVEPOINT-wrapping
    ``record_frame``/``record_narrative`` generally, which is a materially
    larger change than this ticket's persistence-prerequisite scope.
    """

    maker, org_id, user_id = seeded
    question = "How is Nightfall doing?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async def _failing_record_frame(self, *args, **kwargs):
        raise RuntimeError("simulated frame storage failure")

    monkeypatch.setattr(DevPersistenceService, "record_frame", _failing_record_frame)

    async with maker() as session:
        service = DevPersistenceService(session)

        def recorder_factory() -> Recorder:
            return cast(
                Recorder,
                PersistenceRunRecorder(
                    service,
                    org_id=org_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    run_id=run_id,
                    provider_source="platform",
                ),
            )

        output = await run_preflight_orchestrator(
            question=question,
            entities=[(str(org_id), ASK_DEV_PROJECT)],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script=_leaking_but_not_narrating(str(org_id)),
            script_id="chaos-3423-frame-write-failure",
            recorder_factory=recorder_factory,
        )
        await session.commit()

        # Setup control: the frame write really did fail (no dev_answer_frames
        # row), the run still terminated on the same no-answer error, and this
        # is the finish()-internal frame path (frame_already_recorded=False),
        # not the preflight TERMINATE branch's own separate try/except.
        assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
        assert output.result.error is not None
        frame_rows = (
            await session.scalars(
                select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
            )
        ).all()
        assert len(frame_rows) == 0

        assistant_rows = (
            await session.scalars(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
        ).all()
        assert len(assistant_rows) == 1, (
            "a frame-write failure must never discard the already-flushed "
            f"no-answer transcript row -- got {len(assistant_rows)} rows."
        )
        assert assistant_rows[0].content == output.result.error.safe_message


@pytest.mark.asyncio
async def test_chaos_3424_ledger_write_failure_never_strands_the_run(
    seeded, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codex adversarial review (HIGH, confirmed): the new PROCEED ledger
    loop originally called ``append_resolution`` with no failure boundary.
    A database-layer failure on any entry marks the session rollback-only,
    so every later write ``finish()`` makes (record_answer/
    record_error_message/terminal) would raise ``PendingRollbackError`` on
    its own next flush -- turning a routine PROCEED into a stranded,
    non-terminal run over what should be best-effort forensic telemetry.
    Drives a REAL ``DevPersistenceService.append_resolution`` failure and
    asserts the run still reaches its normal graceful terminal with a
    coherent (fully rolled back, never partial) ledger and an intact
    transcript row -- not an unhandled exception, and not a generic
    ``internal_error`` from the outer catch-all.
    """

    maker, org_id, user_id = seeded
    question = "How is Nightfall doing?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async def _always_failing(self, *args, **kwargs):
        raise RuntimeError("simulated resolution ledger storage failure")

    monkeypatch.setattr(DevPersistenceService, "append_resolution", _always_failing)

    async with maker() as session:
        service = DevPersistenceService(session)

        def recorder_factory() -> Recorder:
            return cast(
                Recorder,
                PersistenceRunRecorder(
                    service,
                    org_id=org_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    run_id=run_id,
                    provider_source="platform",
                ),
            )

        output = await run_preflight_orchestrator(
            question=question,
            entities=[(str(org_id), ASK_DEV_PROJECT)],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script=_leaking_but_not_narrating(str(org_id)),
            script_id="chaos-3424-ledger-write-failure",
            recorder_factory=recorder_factory,
        )
        await session.commit()

        # The run must still reach its normal graceful no-answer terminal --
        # never crash, and never degrade to a generic internal_error just
        # because the ledger write failed.
        assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
        assert output.result.error is not None
        assert output.result.error.code == "scope_not_found"
        assert output.result.error.code != "internal_error"

        ledger_rows = (
            await session.scalars(
                select(DevRunResolution).where(DevRunResolution.run_id == run_id)
            )
        ).all()
        assert len(ledger_rows) == 0, (
            "a failed ledger write must roll back cleanly (never a partial "
            f"ledger) -- got {len(ledger_rows)} rows."
        )

        # The run row itself must be a coherent terminal, not stranded
        # non-terminal by a PendingRollbackError the recovery never fixed.
        run_row = await session.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.state == "insufficient_evidence"
        assert run_row.preflight_outcome == "proceeded_unresolved_bare_name"

        # The CHAOS-3423 transcript row still landed -- proof the session
        # recovered cleanly enough for finish()'s own later writes to
        # succeed on it.
        assistant_rows = (
            await session.scalars(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
        ).all()
        assert len(assistant_rows) == 1
        assert assistant_rows[0].content == output.result.error.safe_message


@pytest.mark.asyncio
async def test_chaos_3423_no_answer_row_is_never_treated_as_a_real_answer(
    seeded,
) -> None:
    """Codex adversarial review (MEDIUM, confirmed): the no-answer
    transcript row's ``answer_id`` (the run's own id) satisfies every
    ``role == 'assistant' AND answer_id IS NOT NULL`` query written before
    CHAOS-3423 -- ``DevConversation.latest_answer_id``, ``get_answer_message``
    (evidence expansion), and ``record_feedback`` all have to keep treating
    it as "no real answer here", or a client that dereferences
    ``latest_answer_id`` crashes evidence expansion into a bogus persistence
    error, or attaches helpful/not_helpful feedback to a turn that was never
    answered.
    """

    maker, org_id, user_id = seeded
    question = "What's the status of the Atlas project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)

        def recorder_factory() -> Recorder:
            return cast(
                Recorder,
                PersistenceRunRecorder(
                    service,
                    org_id=org_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    run_id=run_id,
                    provider_source="platform",
                ),
            )

        output = await run_preflight_orchestrator(
            question=question,
            entities=[
                (str(org_id), ATLAS_PROJECT_ONE),
                (str(org_id), ATLAS_PROJECT_TWO),
            ],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="chaos-3423-not-a-real-answer",
            recorder_factory=recorder_factory,
        )
        await session.commit()
        assert output.result.error is not None

        # The no-answer row's answer_id IS the run's own id (see
        # PersistenceRunRecorder.record_error_message) -- confirm every
        # answer-only reader refuses to treat it as a real answer.
        with pytest.raises(DevPersistenceNotFound):
            await service.get_answer_message(
                org_id=org_id, user_id=user_id, answer_id=run_id
            )
        with pytest.raises(DevPersistenceNotFound):
            await service.record_feedback(
                org_id=org_id,
                user_id=user_id,
                answer_id=run_id,
                rating="helpful",
                reasons=[],
            )

        record = await service.get_conversation_record(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )
        assert record.latest_answer_id != run_id
        assert record.latest_answer_id is None

        records = await service.list_conversation_records(
            org_id=org_id, user_id=user_id
        )
        assert len(records) == 1
        assert records[0].latest_answer_id != run_id
        assert records[0].latest_answer_id is None
