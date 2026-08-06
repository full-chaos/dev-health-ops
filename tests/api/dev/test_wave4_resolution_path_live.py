"""CHAOS-3219 Phase 2 Lane 2a: prove ``resolution_path.derive_resolution_path``
against REAL persisted ``dev_run_resolutions`` rows, not hand-built
dataclasses.

Drives the REAL ``DevOrchestrator`` -> real ``PersistenceRunRecorder`` ->
real ``DevPersistenceService`` -> an ephemeral sqlite database, via
``tests._chaos_3292_preflight.run_preflight_orchestrator`` -- the same
production-shaped wiring ``test_chaos_3423_3424_persistence_prerequisites.py``
uses, and for the same reason: a fake recorder's captured call list would
prove nothing about what a corpus-runner query against the real
``dev_run_resolutions`` table actually sees.

Two single-run shapes are provable today with the existing harness (an
unambiguous named-project match, and a two-candidate ambiguity): both are
exercised here. The two-turn alias-disambiguation shape
(``ambiguous_candidates`` then, on a follow-up turn, ``exact_match`` for the
same or a fresh mention) is NOT yet provable live -- Lane 2b has not
authored a scripted multi-turn corpus case, and fabricating one here would
duplicate rather than validate that authoring. That branch's proof today is
the pure-logic unit suite (``tests/acceptance/corpus/test_resolution_path.py``);
this file's docstring says so rather than silently only covering the easy
half.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.persistence import DevPersistenceService
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
from scripts.acceptance.corpus.resolution_path import (
    ResolutionLedgerEntry,
    derive_resolution_path,
)
from tests._chaos_3292_preflight import (
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    Recorder,
    run_preflight_orchestrator,
    scope_dict,
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


@pytest_asyncio.fixture
async def seeded(tmp_path: Any):
    database = tmp_path / "wave4-resolution-path.db"
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


def _recorder_factory(
    service: DevPersistenceService,
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    run_id: uuid.UUID,
):
    def factory() -> Recorder:
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

    return factory


async def _ledger_entries_for_run(
    session: AsyncSession, run_id: uuid.UUID
) -> list[ResolutionLedgerEntry]:
    """The real, on-disk shape a Lane 2a corpus-runner receipt would read.

    ``mention_text`` cannot be read off the row (see the resolution_path
    module docstring) -- the runner supplies it from the case's own known
    mention text. This helper takes it as a lookup keyed by mention_id,
    exactly mirroring how the real runner would cross-reference
    ``subjects.json``.
    """

    rows = (
        await session.scalars(
            select(DevRunResolution)
            .where(DevRunResolution.run_id == run_id)
            .order_by(DevRunResolution.entry_ordinal)
        )
    ).all()
    entries = []
    for row in rows:
        payload = row.payload
        committed = payload.get("committed_entity_ref")
        entries.append(
            ResolutionLedgerEntry(
                outcome=row.outcome,
                mention_id=str(row.mention_id),
                committed_label=committed["display_label"] if committed else None,
            )
        )
    return entries


@pytest.mark.asyncio
async def test_exact_named_project_match_derives_deterministic_exact(seeded) -> None:
    maker, org_id, user_id = seeded
    question = "What's the status of the Ask Dev project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)
        from tests._chaos_3292_preflight import ASK_DEV_PROJECT

        output = await run_preflight_orchestrator(
            question=question,
            entities=[(str(org_id), ASK_DEV_PROJECT)],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="wave4-exact",
            recorder_factory=_recorder_factory(
                service,
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                run_id=run_id,
            ),
        )
        await session.commit()

        # Setup control: a real committed exact match, not some other shape.
        assert output.result.state is RunState.COMPLETED

        entries = await _ledger_entries_for_run(session, run_id)
        assert len(entries) >= 1, "expected at least one real ledger row"
        assert entries[0].outcome == "exact_match"
        # Supply the mention text the way the real runner would: from the
        # case's own known mention list (subjects.json), not the row.
        entries[0] = ResolutionLedgerEntry(
            outcome=entries[0].outcome,
            mention_id=entries[0].mention_id,
            committed_label=entries[0].committed_label,
            mention_text="Ask Dev",
        )
        assert derive_resolution_path(entries) == "deterministic-exact"


@pytest.mark.asyncio
async def test_ambiguous_candidates_derives_miss_clarification(seeded) -> None:
    maker, org_id, user_id = seeded
    question = "What's the status of the Atlas project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)

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
            script_id="wave4-ambiguous",
            recorder_factory=_recorder_factory(
                service,
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                run_id=run_id,
            ),
        )
        await session.commit()

        # Setup control: this really is the needs_clarification shape.
        assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
        assert output.result.error is not None
        assert output.result.error.code == "scope_ambiguous"

        entries = await _ledger_entries_for_run(session, run_id)
        assert len(entries) >= 1
        assert entries[0].outcome == "ambiguous_candidates"
        assert derive_resolution_path(entries) == "miss-clarification"
