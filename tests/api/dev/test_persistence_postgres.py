"""Live PostgreSQL proofs for Ask Dev submission admission.

Point ``DEV_HEALTH_POSTGRES_TEST_URI`` at a SCRATCH database when running this
module locally -- never at the dev ``devhealth`` database. The fixture isolates
itself with ``CREATE SCHEMA`` and drops it on teardown, but a teardown that
does not complete leaves that schema and its tables behind: one such orphan
(``ask_dev_admission_005af009e3ae4442bc8c0f2d3b4e4b0b``, 5 tables) was found
sitting in ``devhealth`` on 2026-08-06, and the fixture's own teardown comment
explains why it can be left there -- a leaked session holding ACCESS SHARE
makes ``DROP SCHEMA ... CASCADE`` wait, and there is no server-side timeout by
default. In CI the target is a throwaway service container, so a leak costs
nothing; on a dev box it accumulates in the database holding real data.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import Table, event, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session

from dev_health_ops.api.dev import terminal_frames
from dev_health_ops.api.dev.persistence import (
    DevAdmissionLimits,
    DevConcurrencyLimitExceeded,
    DevMonthlyCostLimitExceeded,
    DevMonthlyRequestLimitExceeded,
    DevPersistenceService,
    DevPlatformAllowance,
    DevRateLimitExceeded,
)
from dev_health_ops.api.dev.persistence import service as dev_persistence_service
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
from tests._helpers import tables_of

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
# Must stay in step with test_persistence_v2.py's `_TABLES`: the service
# under test writes and reads the whole Ask Dev persistence family, so a
# table missing here is not a narrower fixture, it is an UndefinedTableError
# the moment a test reaches that code path. This list had drifted 8 models
# behind its SQLite sibling, which went unnoticed only because every test
# past the first one in this module was unreachable (the fixture-teardown
# deadlock fixed alongside this).
_TABLES = tables_of(
    Organization,
    User,
    DevConversation,
    DevMessage,
    DevRun,
    DevToolCall,
    DevFeedback,
    DevConversationTombstone,
    DevRunIntent,
    DevRunResolution,
    DevRunSubjectSet,
    DevRunSourceObservation,
    DevAnswerFrame,
    DevRunNarrative,
    DevRunStageDiagnostic,
)

_ERROR_CODE_BY_OUTCOME: dict[str, str] = {
    "not_found": "scope_not_found",
}


def _raw_duplicate_key_frame_payload(*, matching_frame_id: str, run_id: str) -> str:
    """See test_persistence_v2.py's identical helper. Postgres's `->>`
    operator reads the LAST occurrence of a duplicate JSON object key
    (confirmed empirically, same as Python's `json` decoder) -- this
    same raw payload shape that needed an explicit SQLite trigger fix
    (CHAOS-3297 Codex review round 10) is expected to be rejected by the
    round-9 trigger AS-IS on Postgres, with no round-10-specific change,
    because the mismatched duplicate (being LAST) is exactly what
    Postgres's row-binding cross-check reads."""

    return (
        '{"schema_version": "dev_answer_frame.v1", '
        f'"frame_id": "{matching_frame_id}", '
        f'"run_id": "{run_id}", '
        '"public_outcome": "not_found", '
        '"frame_id": "11111111-1111-1111-1111-111111111111"}'
    )


def _raw_duplicate_key_narrative_payload(
    *, matching_narrative_id: str, run_id: str, frame_id: str, mode: str
) -> str:
    return (
        '{"schema_version": "dev_narrative.v1", '
        f'"narrative_id": "{matching_narrative_id}", '
        f'"run_id": "{run_id}", '
        f'"frame_id": "{frame_id}", '
        f'"mode": "{mode}", '
        '"referenced_fact_ids": [], "referenced_section_ids": [], '
        '"provider_metadata": null, '
        f'"generated_at": "{datetime.now(UTC).isoformat()}", '
        '"validation_warnings": [], '
        '"narrative_id": "11111111-1111-1111-1111-111111111111"}'
    )


def _frame_payload(*, run_id: uuid.UUID, outcome: str) -> dict[str, Any]:
    """A valid ``dev_answer_frame.v1`` no-answer payload, its own
    ``run_id``/``public_outcome`` matching what ``record_frame`` cross-
    checks against its own arguments -- mirrors
    ``test_persistence_v2.py``'s identical helper, kept self-contained
    here since this module runs standalone against a live database."""

    frame = terminal_frames.build_error_frame(
        code=_ERROR_CODE_BY_OUTCOME[outcome],
        run_id=str(run_id),
        generated_at=datetime.now(UTC),
    )
    return frame.model_dump(mode="json")


@contextmanager
def _session_level_payload_guards_disabled():
    """See ``test_persistence_v2.py``'s identical helper: temporarily
    unregisters every session-level payload guard (mapper events +
    do_orm_execute) so a test can prove the DB trigger -- not this layer
    -- is what rejects a given write. The listeners were registered with
    direct references to the original functions at import time, so a
    monkeypatched module attribute would not affect them; ``event.remove``
    is the only way to genuinely disable them, and it always restores."""

    at_flush = dev_persistence_service._enforce_payload_contract_at_flush
    on_bulk_dml = dev_persistence_service._enforce_payload_contract_on_bulk_dml
    payload_models = list(dev_persistence_service._PAYLOAD_MODEL_VALIDATORS)
    for model in payload_models:
        event.remove(model, "before_insert", at_flush)
        event.remove(model, "before_update", at_flush)
    event.remove(Session, "do_orm_execute", on_bulk_dml)
    try:
        yield
    finally:
        for model in payload_models:
            event.listen(model, "before_insert", at_flush)
            event.listen(model, "before_update", at_flush)
        event.listen(Session, "do_orm_execute", on_bulk_dml)


def _require_postgres_test_uri() -> None:
    """Same contract as ``tests/api/admin/test_add_member_visibility.py``'s
    ``_require_postgres_test_uri`` (CHAOS-3411): skip locally when the URI is
    not configured, but hard-``pytest.fail`` under CI.

    This module used a plain module-level ``skipif`` (CHAOS-3441 Codex round 1
    follow-up), which is the same silent-skip trap CHAOS-3411 closed for its
    own module, and it had been open here the whole time: the PR-gated unit
    step (`.github/workflows/test.yml`, "Run parallel unit test contract")
    never sets this URI, the PR-gated "Run PostgreSQL migration tests" step
    named four files and not this one, and the only job that does set it
    (`coverage`) is gated `if: github.event_name != 'pull_request'`. So all
    of this module took the skip branch on every PR -- not just CHAOS-3441's
    two savepoint proofs, but the CHAOS-3297/3325 duplicate-key and NUL-alias
    trigger proofs that have guarded the payload contract since they landed.
    A stripped commit would have passed the required gate. Missing coverage
    under CI must be a loud failure, not a silent pass -- and the module is
    now named in that step's file list so CI genuinely runs it.
    """

    if os.getenv(_POSTGRES_URI_ENV):
        return
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        pytest.fail(
            f"{_POSTGRES_URI_ENV} must be configured for the Ask Dev "
            "persistence PostgreSQL proofs"
        )
    pytest.skip(f"requires {_POSTGRES_URI_ENV}")


@pytest.fixture(autouse=True, scope="module")
def require_postgres_test_uri() -> None:
    _require_postgres_test_uri()


@pytest_asyncio.fixture
async def postgres_persistence() -> AsyncIterator[
    tuple[async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID]
]:
    configured_url = make_url(os.environ[_POSTGRES_URI_ENV])
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")
    async_url = configured_url.set(drivername="postgresql+asyncpg")
    schema = f"ask_dev_admission_{uuid.uuid4().hex}"
    admin_engine = create_async_engine(async_url)
    engine: AsyncEngine | None = None
    schema_created = False
    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            schema_created = True
        engine = create_async_engine(
            async_url,
            connect_args={"server_settings": {"search_path": schema}},
        )
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=_TABLES,
                )
            )
        maker = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        org_id = uuid.uuid4()
        user_id = uuid.uuid4()
        async with maker() as session:
            session.add_all(
                [
                    Organization(
                        id=org_id,
                        slug=f"ask-dev-admission-{org_id.hex}",
                        name="Ask Dev admission",
                    ),
                    User(
                        id=user_id,
                        email=f"ask-dev-admission-{user_id.hex}@example.com",
                    ),
                ]
            )
            await session.commit()
        yield maker, engine, org_id, user_id
    finally:
        if engine is not None:
            await engine.dispose()
        if schema_created:
            async with admin_engine.begin() as connection:
                # DROP SCHEMA ... CASCADE needs ACCESS EXCLUSIVE on every table
                # in the schema, so a session this test leaked still-open (a
                # query issued outside its `async with`, leaving the backend
                # `idle in transaction` holding ACCESS SHARE) makes this
                # statement wait forever. There is no server-side timeout by
                # default, so that hang is unbounded: under pytest-xdist it
                # silently wedges one worker inside fixture teardown, the
                # controller then blocks forever waiting for a report that
                # never comes, and the whole CI job burns its 6-hour limit
                # with no failure ever attributed to this file. Bound the wait
                # so the leak fails loudly and points here instead.
                await connection.execute(text("SET LOCAL lock_timeout = '30s'"))
                await connection.execute(
                    text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                )
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_postgres_serializes_concurrent_user_admission_and_replay_is_free(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, engine, org_id, user_id = postgres_persistence
    async with maker() as seed_session:
        seed_service = DevPersistenceService(seed_session)
        conversations = [
            await seed_service.create_conversation(
                org_id=org_id,
                user_id=user_id,
                current_scope={},
            )
            for _ in range(2)
        ]
        await seed_session.commit()

    limits = DevAdmissionLimits(
        active_runs_per_user=1,
        requests_per_user_per_15_minutes=2,
        requests_per_org_per_hour=2,
    )
    client_message_ids = [uuid.uuid4(), uuid.uuid4()]
    both_org_locks_attempted = asyncio.Event()
    org_lock_attempts = 0
    locks_by_connection: dict[int, list[str]] = defaultdict(list)

    async with maker() as coordinator:
        await coordinator.scalar(
            select(Organization.id).where(Organization.id == org_id).with_for_update()
        )

        @event.listens_for(engine.sync_engine, "before_cursor_execute")
        def _record_for_update(
            connection: Any,
            _cursor: Any,
            statement: str,
            _parameters: Any,
            _context: Any,
            _executemany: bool,
        ) -> None:
            nonlocal org_lock_attempts
            normalized = " ".join(statement.lower().split())
            if " for update" not in normalized:
                return
            for table_name in ("organizations", "users", "dev_conversations"):
                if f"from {table_name}" in normalized:
                    locks_by_connection[id(connection)].append(table_name)
                    if table_name == "organizations":
                        org_lock_attempts += 1
                        if org_lock_attempts == 2:
                            both_org_locks_attempted.set()
                    break

        start = asyncio.Event()

        async def _submit(index: int) -> tuple[str, Any]:
            async with maker() as session:
                await start.wait()
                service = DevPersistenceService(session)
                try:
                    result = await service.append_user_message_and_run(
                        org_id=org_id,
                        user_id=user_id,
                        conversation_id=conversations[index].id,
                        client_message_id=client_message_ids[index],
                        question=f"Concurrent question {index}",
                        scope_snapshot={},
                        admission_limits=limits,
                    )
                    await session.commit()
                    return "accepted", result
                except DevConcurrencyLimitExceeded as exc:
                    await session.rollback()
                    return "rejected", exc

        submissions = [asyncio.create_task(_submit(index)) for index in range(2)]
        try:
            start.set()
            await asyncio.wait_for(both_org_locks_attempted.wait(), timeout=5)
            assert all(not submission.done() for submission in submissions)
            await coordinator.commit()
            outcomes = await asyncio.gather(*submissions)
        finally:
            event.remove(
                engine.sync_engine,
                "before_cursor_execute",
                _record_for_update,
            )
            for submission in submissions:
                if not submission.done():
                    submission.cancel()
            await asyncio.gather(*submissions, return_exceptions=True)

    assert sorted(outcome for outcome, _result in outcomes) == [
        "accepted",
        "rejected",
    ]
    assert sorted(locks_by_connection.values()) == [
        ["organizations", "users", "dev_conversations"],
        ["organizations", "users", "dev_conversations"],
    ]

    accepted_index = next(
        index
        for index, (outcome, _result) in enumerate(outcomes)
        if outcome == "accepted"
    )
    accepted = outcomes[accepted_index][1]
    async with maker() as replay_session:
        replay_service = DevPersistenceService(replay_session)
        replay = await replay_service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversations[accepted_index].id,
            client_message_id=client_message_ids[accepted_index],
            question="A replay must not replace or consume quota.",
            scope_snapshot={},
            admission_limits=limits,
        )
        assert replay.created is False
        assert replay.message.id == accepted.message.id
        assert replay.run.id == accepted.run.id
        assert replay.message.content == f"Concurrent question {accepted_index}"
        assert await replay_session.scalar(select(func.count(DevRun.id))) == 1
        assert await replay_session.scalar(select(func.count(DevMessage.id))) == 1
        await replay_service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=accepted.run.id,
            state="cancelled",
        )
        await replay_session.commit()

    second_client_message_id = uuid.uuid4()
    async with maker() as second_session:
        second_service = DevPersistenceService(second_session)
        second = await second_service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversations[1 - accepted_index].id,
            client_message_id=second_client_message_id,
            question="The second request still fits the two-request budget.",
            scope_snapshot={},
            admission_limits=limits,
        )
        assert second.created is True
        await second_service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=second.run.id,
            state="cancelled",
        )
        await second_session.commit()

    async with maker() as exhausted_session:
        with pytest.raises(DevRateLimitExceeded):
            await DevPersistenceService(exhausted_session).append_user_message_and_run(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversations[accepted_index].id,
                client_message_id=uuid.uuid4(),
                question="A third unique request exceeds the two-request budget.",
                scope_snapshot={},
                admission_limits=limits,
            )
        await exhausted_session.rollback()

    async with maker() as verify_session:
        assert await verify_session.scalar(select(func.count(DevRun.id))) == 2
        # Both counts must stay INSIDE the `async with`. Querying
        # verify_session after the block exits silently opens a brand-new
        # transaction on a session nothing will ever close or roll back,
        # leaving the Postgres backend `idle in transaction` with an ACCESS
        # SHARE lock on dev_messages -- which then blocks this fixture's
        # teardown DROP SCHEMA ... CASCADE indefinitely.
        assert await verify_session.scalar(select(func.count(DevMessage.id))) == 2


@pytest.mark.asyncio
async def test_platform_monthly_allowance_charges_unique_runs_and_replay_is_free(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={},
        )
        allowance = DevPlatformAllowance(
            monthly_request_limit=2,
            monthly_cost_limit_microusd=6_000_000,
        )
        client_message_id = uuid.uuid4()
        first = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=client_message_id,
            question="First platform request",
            scope_snapshot={},
            admission_limits=DevAdmissionLimits(),
            provider_source="platform",
            platform_allowance=allowance,
        )
        replay = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=client_message_id,
            question="Replay content is ignored",
            scope_snapshot={},
            admission_limits=DevAdmissionLimits(),
            provider_source="platform",
            platform_allowance=allowance,
        )
        assert replay.created is False
        assert replay.run.id == first.run.id
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=first.run.id,
            state="completed",
            estimated_cost_microusd=1_000_000,
        )
        second = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="Second platform request",
            scope_snapshot={},
            admission_limits=DevAdmissionLimits(),
            provider_source="platform",
            platform_allowance=allowance,
        )
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=second.run.id,
            state="cancelled",
        )
        with pytest.raises(DevMonthlyRequestLimitExceeded):
            await service.append_user_message_and_run(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation.id,
                client_message_id=uuid.uuid4(),
                question="Third platform request",
                scope_snapshot={},
                admission_limits=DevAdmissionLimits(),
                provider_source="platform",
                platform_allowance=allowance,
            )


@pytest.mark.asyncio
async def test_platform_monthly_allowance_reserves_unknown_cost_fail_closed(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id,
            user_id=user_id,
            current_scope={},
        )
        allowance = DevPlatformAllowance(
            monthly_request_limit=10,
            monthly_cost_limit_microusd=5_000_000,
        )
        first = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            client_message_id=uuid.uuid4(),
            question="Reserve the full unknown platform cost",
            scope_snapshot={},
            provider_source="platform",
            platform_allowance=allowance,
        )
        await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=first.run.id,
            state="cancelled",
        )
        with pytest.raises(DevMonthlyCostLimitExceeded):
            await service.append_user_message_and_run(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation.id,
                client_message_id=uuid.uuid4(),
                question="Unknown cost cannot be treated as zero",
                scope_snapshot={},
                provider_source="platform",
                platform_allowance=allowance,
            )


# -- CHAOS-3297 Codex review round 9: the DB trigger closure, production
# dialect. The upsert representation differs per dialect (`ON CONFLICT DO
# UPDATE` compiles differently for Postgres vs. SQLite even though the
# SQLAlchemy construct looks similar), so the same pair of Codex repros
# proven against SQLite in test_persistence_v2.py is repeated here against
# a live PostgreSQL database -- this is what production actually runs.


@pytest.mark.asyncio
async def test_postgres_db_trigger_rejects_a_core_table_update_with_no_bind_mapper(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    """Codex round 9 repro 1/2, PostgreSQL: ``session.execute(update(
    DevAnswerFrame.__table__)...)`` -- a Core-table UPDATE against the
    bare ``Table``, not the mapped class, has no ``bind_mapper`` for the
    session-level guard to look up. Run with the guards disabled first
    (proving the trigger alone rejects it), then restored.
    """

    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        with _session_level_payload_guards_disabled():
            with pytest.raises(IntegrityError, match="dev_answer_frames"):
                await session.execute(
                    update(cast(Table, DevAnswerFrame.__table__))
                    .where(DevAnswerFrame.__table__.c.id == frame_id)
                    .values(payload={"schema_version": "dev_answer_frame.v1"})
                )
            await session.rollback()

        async with maker() as check_session:
            reloaded = await check_session.get(DevAnswerFrame, frame_id)
            assert reloaded is not None
            assert reloaded.payload == valid_payload

        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await session.execute(
                update(cast(Table, DevAnswerFrame.__table__))
                .where(DevAnswerFrame.__table__.c.id == frame_id)
                .values(payload={"schema_version": "dev_answer_frame.v1"})
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload


@pytest.mark.asyncio
async def test_postgres_db_trigger_rejects_an_on_conflict_do_update_set_clause(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    """Codex round 9 repro 2/2, PostgreSQL: ``insert(...)
    .on_conflict_do_update(..., set_={"payload": ...})`` -- the INSERT's
    own values are whatever the session-level guard validates, but the
    conflict resolution's SET clause is a separate part of the compiled
    statement nothing in that layer inspects. Two variants against the
    SAME conflicting row: a malformed payload, and a fully valid payload
    for a completely different run (fails the row-binding cross-check).
    Run with the guards disabled first, then restored.
    """

    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        # Same run_id as the existing frame -- the unique constraint on
        # run_id is what turns this INSERT into an upsert against the row
        # above. The INSERT's own payload is fully valid on its own.
        insert_payload = _frame_payload(run_id=run_id, outcome="not_found")
        alien_run_id = uuid.uuid4()
        alien_payload = dict(valid_payload)
        alien_payload["run_id"] = str(alien_run_id)

        def _upsert_statement(conflict_payload: dict[str, Any]) -> Any:
            return (
                postgresql_insert(DevAnswerFrame)
                .values(
                    id=uuid.uuid4(),
                    run_id=run_id,
                    org_id=org_id,
                    user_id=user_id,
                    frame_id=uuid.UUID(insert_payload["frame_id"]),
                    public_outcome="not_found",
                    payload=insert_payload,
                    created_at=datetime.now(UTC),
                )
                .on_conflict_do_update(
                    index_elements=["run_id"],
                    set_={"payload": conflict_payload},
                )
            )

        with _session_level_payload_guards_disabled():
            for conflict_payload, label in (
                ({"schema_version": "dev_answer_frame.v1"}, "malformed"),
                (alien_payload, "alien-but-internally-valid"),
            ):
                with pytest.raises(IntegrityError, match="dev_answer_frames"):
                    await session.execute(_upsert_statement(conflict_payload))
                await session.rollback()
                async with maker() as check_session:
                    reloaded = await check_session.get(DevAnswerFrame, frame_id)
                    assert reloaded is not None
                    assert reloaded.payload == valid_payload, (
                        f"trigger-alone ({label}): the conflicting row's "
                        f"original payload must be untouched"
                    )

        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await session.execute(_upsert_statement(alien_payload))
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.run_id == run_id)
        )
        assert count == 1


# -- CHAOS-3297 Codex review round 10 MEDIUM, PostgreSQL confirmation:
# `json_extract` on SQLite reads the FIRST occurrence of a duplicate JSON
# object key; Postgres's `->>` operator (like Python's `json` decoder)
# reads the LAST -- confirmed empirically. These tests assert Postgres
# ALREADY rejects the same raw duplicate-key payload shape that needed an
# explicit SQLite trigger fix, with no round-10-specific Postgres change
# at all -- pinning the equivalence in both directions: SQLite needed a
# patch, Postgres's existing round-9 row-binding cross-check already
# reads the mismatched (last) copy and rejects on its own.


@pytest.mark.asyncio
async def test_postgres_trigger_already_rejects_duplicate_key_frame_insert(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        connection = await session.connection()
        raw_payload = _raw_duplicate_key_frame_payload(
            matching_frame_id=valid_payload["frame_id"], run_id=str(run_id)
        )
        new_id = uuid.uuid4()
        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await connection.execute(
                text(
                    "INSERT INTO dev_answer_frames "
                    "(id, run_id, org_id, user_id, frame_id, public_outcome, "
                    "payload, created_at) VALUES "
                    "(:id, :run_id, :org_id, :user_id, :frame_id, :public_outcome, "
                    ":payload, :created_at)"
                ),
                {
                    "id": new_id,
                    "run_id": run_id,
                    "org_id": org_id,
                    "user_id": user_id,
                    "frame_id": uuid.UUID(valid_payload["frame_id"]),
                    "public_outcome": "not_found",
                    "payload": raw_payload,
                    "created_at": datetime.now(UTC),
                },
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.id == new_id)
        )
        assert count == 0, "the duplicate-key insert must not have created a row"


@pytest.mark.asyncio
async def test_postgres_trigger_already_rejects_duplicate_key_frame_update(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        connection = await session.connection()
        raw_payload = _raw_duplicate_key_frame_payload(
            matching_frame_id=valid_payload["frame_id"], run_id=str(run_id)
        )
        with pytest.raises(IntegrityError, match="dev_answer_frames"):
            await connection.execute(
                text("UPDATE dev_answer_frames SET payload = :payload WHERE id = :id"),
                {"payload": raw_payload, "id": frame_id},
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload, (
            "the duplicate-key update must leave the original payload untouched"
        )


@pytest.mark.asyncio
async def test_postgres_trigger_already_rejects_duplicate_key_narrative_insert(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        frame_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="not_found",
            payload=frame_payload,
        )
        await session.commit()

        narrative_id = uuid.uuid4()
        connection = await session.connection()
        raw_payload = _raw_duplicate_key_narrative_payload(
            matching_narrative_id=str(narrative_id),
            run_id=str(run_id),
            frame_id=str(frame.frame_id),
            mode="deterministic_fallback",
        )
        new_id = uuid.uuid4()
        with pytest.raises(IntegrityError, match="dev_run_narratives"):
            await connection.execute(
                text(
                    "INSERT INTO dev_run_narratives "
                    "(id, run_id, org_id, user_id, narrative_id, frame_id, mode, "
                    "provider_fingerprint, narrative_text, payload, created_at) VALUES "
                    "(:id, :run_id, :org_id, :user_id, :narrative_id, :frame_id, :mode, "
                    ":provider_fingerprint, :narrative_text, :payload, :created_at)"
                ),
                {
                    "id": new_id,
                    "run_id": run_id,
                    "org_id": org_id,
                    "user_id": user_id,
                    "narrative_id": narrative_id,
                    "frame_id": frame.frame_id,
                    "mode": "deterministic_fallback",
                    "provider_fingerprint": None,
                    "narrative_text": "A safe presentation summary.",
                    "payload": raw_payload,
                    "created_at": datetime.now(UTC),
                },
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevRunNarrative.id)).where(DevRunNarrative.id == new_id)
        )
        assert count == 0, "the duplicate-key insert must not have created a row"


@pytest.mark.asyncio
async def test_postgres_trigger_already_rejects_duplicate_key_narrative_update(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        frame_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="not_found",
            payload=frame_payload,
        )
        narrative_id = uuid.uuid4()
        narrative_text = "A safe presentation summary."
        narrative_payload: dict[str, Any] = {
            "schema_version": "dev_narrative.v1",
            "narrative_id": str(narrative_id),
            "run_id": str(run_id),
            "frame_id": str(frame.frame_id),
            "mode": "deterministic_fallback",
            "referenced_fact_ids": [],
            "referenced_section_ids": [],
            "provider_metadata": None,
            "generated_at": datetime.now(UTC).isoformat(),
            "validation_warnings": [],
        }
        narrative = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=narrative_id,
            frame_id=frame.frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=None,
            narrative_text=narrative_text,
            payload=narrative_payload,
        )
        await session.commit()
        narrative_pk = narrative.id

        connection = await session.connection()
        raw_payload = _raw_duplicate_key_narrative_payload(
            matching_narrative_id=str(narrative_id),
            run_id=str(run_id),
            frame_id=str(frame.frame_id),
            mode="deterministic_fallback",
        )
        with pytest.raises(IntegrityError, match="dev_run_narratives"):
            await connection.execute(
                text("UPDATE dev_run_narratives SET payload = :payload WHERE id = :id"),
                {"payload": raw_payload, "id": narrative_pk},
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevRunNarrative, narrative_pk)
        assert reloaded is not None
        assert reloaded.narrative_text == narrative_text


# -- CHAOS-3297 Codex review round 11 HIGH, PostgreSQL confirmation: the
# SQLite json_extract path-matching NUL-truncation quirk (round 11's fix
# in models/dev_persistence.py) has no Postgres analogue at all, and
# needed no round-11-specific Postgres change. Confirmed empirically:
# Postgres's ->> operator, when the JSON document contains a NUL-escaped
# key ANYWHERE, raises UntranslatableCharacterError (SQLSTATE 22P05 -- a
# NUL byte cannot be represented in Postgres's text type) the moment the
# document is decoded to extract ANY key -- not merely a theoretical
# "reads full label" difference from SQLite; a NUL-aliased payload is
# structurally unrepresentable as Postgres text at all. This surfaces as
# sqlalchemy.exc.DBAPIError (asyncpg maps UntranslatableCharacterError's
# base PostgresError to the generic dialect Error, not specifically
# IntegrityError) -- these tests assert THAT, pinning the exact
# exception shape so a future SQLAlchemy/asyncpg version change would
# fail loudly here rather than silently stop proving this.


def _raw_nul_alias_frame_payload(*, matching_frame_id: str, run_id: str) -> str:
    return (
        '{"frame_id\\u0000XXXX": "' + matching_frame_id + '", '
        '"schema_version": "dev_answer_frame.v1", '
        f'"run_id": "{run_id}", '
        '"public_outcome": "not_found", '
        '"frame_id": "11111111-1111-1111-1111-111111111111"}'
    )


def _raw_nul_alias_narrative_payload(
    *, matching_narrative_id: str, run_id: str, frame_id: str, mode: str
) -> str:
    return (
        '{"narrative_id\\u0000XXXX": "' + matching_narrative_id + '", '
        '"schema_version": "dev_narrative.v1", '
        f'"run_id": "{run_id}", '
        f'"frame_id": "{frame_id}", '
        f'"mode": "{mode}", '
        '"referenced_fact_ids": [], "referenced_section_ids": [], '
        '"provider_metadata": null, '
        f'"generated_at": "{datetime.now(UTC).isoformat()}", '
        '"validation_warnings": [], '
        '"narrative_id": "11111111-1111-1111-1111-111111111111"}'
    )


@pytest.mark.asyncio
async def test_postgres_already_rejects_nul_alias_frame_insert(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        connection = await session.connection()
        raw_payload = _raw_nul_alias_frame_payload(
            matching_frame_id=valid_payload["frame_id"], run_id=str(run_id)
        )
        new_id = uuid.uuid4()
        with pytest.raises(DBAPIError, match="[Uu]nicode|NUL|22P05"):
            await connection.execute(
                text(
                    "INSERT INTO dev_answer_frames "
                    "(id, run_id, org_id, user_id, frame_id, public_outcome, "
                    "payload, created_at) VALUES "
                    "(:id, :run_id, :org_id, :user_id, :frame_id, :public_outcome, "
                    ":payload, :created_at)"
                ),
                {
                    "id": new_id,
                    "run_id": run_id,
                    "org_id": org_id,
                    "user_id": user_id,
                    "frame_id": uuid.UUID(valid_payload["frame_id"]),
                    "public_outcome": "not_found",
                    "payload": raw_payload,
                    "created_at": datetime.now(UTC),
                },
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevAnswerFrame.id)).where(DevAnswerFrame.id == new_id)
        )
        assert count == 0, "the NUL-alias insert must not have created a row"


@pytest.mark.asyncio
async def test_postgres_already_rejects_nul_alias_frame_update(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        valid_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(valid_payload["frame_id"]),
            public_outcome="not_found",
            payload=valid_payload,
        )
        await session.commit()
        frame_id = frame.id

        connection = await session.connection()
        raw_payload = _raw_nul_alias_frame_payload(
            matching_frame_id=valid_payload["frame_id"], run_id=str(run_id)
        )
        with pytest.raises(DBAPIError, match="[Uu]nicode|NUL|22P05"):
            await connection.execute(
                text("UPDATE dev_answer_frames SET payload = :payload WHERE id = :id"),
                {"payload": raw_payload, "id": frame_id},
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevAnswerFrame, frame_id)
        assert reloaded is not None
        assert reloaded.payload == valid_payload, (
            "the NUL-alias update must leave the original payload untouched"
        )


@pytest.mark.asyncio
async def test_postgres_already_rejects_nul_alias_narrative_insert(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        frame_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="not_found",
            payload=frame_payload,
        )
        await session.commit()

        narrative_id = uuid.uuid4()
        connection = await session.connection()
        raw_payload = _raw_nul_alias_narrative_payload(
            matching_narrative_id=str(narrative_id),
            run_id=str(run_id),
            frame_id=str(frame.frame_id),
            mode="deterministic_fallback",
        )
        new_id = uuid.uuid4()
        with pytest.raises(DBAPIError, match="[Uu]nicode|NUL|22P05"):
            await connection.execute(
                text(
                    "INSERT INTO dev_run_narratives "
                    "(id, run_id, org_id, user_id, narrative_id, frame_id, mode, "
                    "provider_fingerprint, narrative_text, payload, created_at) VALUES "
                    "(:id, :run_id, :org_id, :user_id, :narrative_id, :frame_id, :mode, "
                    ":provider_fingerprint, :narrative_text, :payload, :created_at)"
                ),
                {
                    "id": new_id,
                    "run_id": run_id,
                    "org_id": org_id,
                    "user_id": user_id,
                    "narrative_id": narrative_id,
                    "frame_id": frame.frame_id,
                    "mode": "deterministic_fallback",
                    "provider_fingerprint": None,
                    "narrative_text": "A safe presentation summary.",
                    "payload": raw_payload,
                    "created_at": datetime.now(UTC),
                },
            )
        await session.rollback()

    async with maker() as session:
        count = await session.scalar(
            select(func.count(DevRunNarrative.id)).where(DevRunNarrative.id == new_id)
        )
        assert count == 0, "the NUL-alias insert must not have created a row"


@pytest.mark.asyncio
async def test_postgres_already_rejects_nul_alias_narrative_update(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    maker, _engine, org_id, user_id = postgres_persistence
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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        frame_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="not_found",
            payload=frame_payload,
        )
        narrative_id = uuid.uuid4()
        narrative_text = "A safe presentation summary."
        narrative_payload: dict[str, Any] = {
            "schema_version": "dev_narrative.v1",
            "narrative_id": str(narrative_id),
            "run_id": str(run_id),
            "frame_id": str(frame.frame_id),
            "mode": "deterministic_fallback",
            "referenced_fact_ids": [],
            "referenced_section_ids": [],
            "provider_metadata": None,
            "generated_at": datetime.now(UTC).isoformat(),
            "validation_warnings": [],
        }
        narrative = await service.record_narrative(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            narrative_id=narrative_id,
            frame_id=frame.frame_id,
            mode="deterministic_fallback",
            provider_fingerprint=None,
            narrative_text=narrative_text,
            payload=narrative_payload,
        )
        await session.commit()
        narrative_pk = narrative.id

        connection = await session.connection()
        raw_payload = _raw_nul_alias_narrative_payload(
            matching_narrative_id=str(narrative_id),
            run_id=str(run_id),
            frame_id=str(frame.frame_id),
            mode="deterministic_fallback",
        )
        with pytest.raises(DBAPIError, match="[Uu]nicode|NUL|22P05"):
            await connection.execute(
                text("UPDATE dev_run_narratives SET payload = :payload WHERE id = :id"),
                {"payload": raw_payload, "id": narrative_pk},
            )
        await session.rollback()

    async with maker() as session:
        reloaded = await session.get(DevRunNarrative, narrative_pk)
        assert reloaded is not None
        assert reloaded.narrative_text == narrative_text


@pytest.mark.asyncio
async def test_chaos_3441_savepoint_recovers_an_aborted_transaction(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    """CHAOS-3441 Codex adversarial review rounds 1-2 (HIGH): the real proof,
    on the real engine.

    The failure this ticket closes is PostgreSQL-specific and sqlite cannot
    express it: a server-side error on ANY statement aborts the whole
    transaction, so every later statement raises ``InFailedSqlTransaction``
    and the caller's already-flushed transcript row dies with the commit that
    can no longer happen. ``ROLLBACK TO SAVEPOINT`` is what makes an aborted
    transaction usable again, and it can only do that if the SAVEPOINT was
    emitted BEFORE the failing statement. The sqlite suite can only observe
    statement ORDER
    (``test_chaos_3441_savepoint_opens_before_the_pre_write_selects``); this
    test observes the semantics.

    The fault is injected into ``record_frame``'s own ownership SELECT --
    the statement that used to run outside the savepoint -- by rewriting it
    into one PostgreSQL rejects server-side (a division by zero). The server
    raises it, not the test harness, and the transaction is genuinely
    aborted. What must survive: the ``DevAnswer`` transcript row and the
    stage diagnostic flushed before the call, plus a usable session for
    ``terminal()``'s own ``update_run``.
    """

    maker, engine, org_id, user_id = postgres_persistence
    answer_id = uuid.uuid4()

    fault_armed = {"value": False}

    @event.listens_for(engine.sync_engine, "before_cursor_execute", retval=True)
    def _abort_the_ownership_select(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> tuple[str, Any]:
        if (
            fault_armed["value"]
            and statement.lstrip().upper().startswith("SELECT")
            and "FROM dev_runs" in statement
        ):
            # Still a real statement the server parses, plans and REJECTS --
            # not an exception raised in-process before the driver runs.
            return statement + " AND 1 / 0 = 0", parameters
        return statement, parameters

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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        await service.append_assistant_answer(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            answer_payload={
                "schema_version": "dev_answer.v1",
                "answer_id": str(answer_id),
                "conversation_id": str(conversation.id),
                "summary": "The evidence-backed answer can be rendered from storage.",
                "claims": [],
                "metrics": [],
                "evidence": [],
            },
            validator=lambda payload: payload,
            scope_snapshot={},
        )
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

        payload = _frame_payload(run_id=run_id, outcome="not_found")
        fault_armed["value"] = True
        try:
            with pytest.raises(DBAPIError) as raised:
                await service.record_frame(
                    org_id=org_id,
                    user_id=user_id,
                    run_id=run_id,
                    frame_id=uuid.UUID(payload["frame_id"]),
                    public_outcome="not_found",
                    payload=payload,
                )
        finally:
            fault_armed["value"] = False
        assert "division by zero" in str(raised.value.orig).lower()

        # The transaction was aborted by the server and recovered by the
        # savepoint: this write lands with no session.rollback() in between,
        # exactly as orchestrator.finish() proceeds after a frame failure.
        run = await service.update_run(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            state="completed",
            answer_id=answer_id,
            provider_source="platform",
            provider_fingerprint="sha256:" + ("a" * 64),
            model_fingerprint="sha256:" + ("a" * 64),
            prompt_version="dev-system.v1",
            tool_contract_version="dev-tools.v1",
            metric_version="metric-registry.v1",
            query_version="dev-query.v1",
        )
        assert run is not None
        await session.commit()

    async with maker() as reader:
        messages = (
            await reader.scalars(
                select(DevMessage).where(
                    DevMessage.conversation_id == conversation.id,
                    DevMessage.role == "assistant",
                )
            )
        ).all()
        assert [m.answer_id for m in messages] == [answer_id], (
            "the flushed transcript row must survive a server-aborted "
            "transaction inside record_frame"
        )
        diagnostics = (
            await reader.scalars(
                select(DevRunStageDiagnostic).where(
                    DevRunStageDiagnostic.run_id == run_id
                )
            )
        ).all()
        assert [d.stage_id for d in diagnostics] == ["resolving_subjects"]
        run_row = await reader.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.state == "completed"
        assert run_row.contract_generation == "v1"
        assert (
            await reader.scalar(
                select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
            )
        ) is None


@pytest.mark.asyncio
async def test_chaos_3441_narrative_savepoint_recovers_an_aborted_transaction(
    postgres_persistence: tuple[
        async_sessionmaker[AsyncSession], AsyncEngine, uuid.UUID, uuid.UUID
    ],
) -> None:
    """The same proof for ``record_narrative`` (Codex adversarial review
    round 3: the frame proof did not cover it).

    A narrative failure is the worst place to lose the outer transaction --
    by the time it runs, the transcript row AND the frame are already
    flushed on it -- so its savepoint has to open before its own ownership
    and frame-lookup SELECTs, and be able to recover a transaction the
    server has aborted. Fault injected into the frame-lookup SELECT this
    time, so the proof covers a different statement than the frame test.
    """

    maker, engine, org_id, user_id = postgres_persistence
    fault_armed = {"value": False}

    @event.listens_for(engine.sync_engine, "before_cursor_execute", retval=True)
    def _abort_the_frame_lookup(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> tuple[str, Any]:
        if (
            fault_armed["value"]
            and statement.lstrip().upper().startswith("SELECT")
            and "FROM dev_answer_frames" in statement
        ):
            return statement + " AND 1 / 0 = 0", parameters
        return statement, parameters

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
            question="What is the status of this project?",
            scope_snapshot={},
        )
        await session.commit()
        run_id = accepted.run.id

        frame_payload = _frame_payload(run_id=run_id, outcome="not_found")
        frame = await service.record_frame(
            org_id=org_id,
            user_id=user_id,
            run_id=run_id,
            frame_id=uuid.UUID(frame_payload["frame_id"]),
            public_outcome="not_found",
            payload=frame_payload,
        )

        narrative_id = uuid.uuid4()
        narrative_payload: dict[str, Any] = {
            "schema_version": "dev_narrative.v1",
            "narrative_id": str(narrative_id),
            "run_id": str(run_id),
            "frame_id": str(frame.frame_id),
            "mode": "deterministic_fallback",
            "referenced_fact_ids": [],
            "referenced_section_ids": [],
            "provider_metadata": None,
            "generated_at": datetime.now(UTC).isoformat(),
            "validation_warnings": [],
        }

        fault_armed["value"] = True
        try:
            with pytest.raises(DBAPIError) as raised:
                await service.record_narrative(
                    org_id=org_id,
                    user_id=user_id,
                    run_id=run_id,
                    narrative_id=narrative_id,
                    frame_id=frame.frame_id,
                    mode="deterministic_fallback",
                    provider_fingerprint=None,
                    narrative_text="A safe presentation summary.",
                    payload=narrative_payload,
                )
        finally:
            fault_armed["value"] = False
        assert "division by zero" in str(raised.value.orig).lower()

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
        # The frame flushed before the aborted narrative call survives, and
        # its run reached a terminal state on the same session.
        surviving = await reader.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
        )
        assert surviving is not None
        assert surviving.frame_id == frame.frame_id
        assert (
            await reader.scalar(
                select(DevRunNarrative).where(DevRunNarrative.run_id == run_id)
            )
        ) is None
        run_row = await reader.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.state == "failed"
        assert run_row.contract_generation == "v2"
