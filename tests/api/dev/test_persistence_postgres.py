"""Live PostgreSQL proofs for Ask Dev submission admission."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dev_health_ops.api.dev.persistence import (
    DevAdmissionLimits,
    DevConcurrencyLimitExceeded,
    DevMonthlyCostLimitExceeded,
    DevMonthlyRequestLimitExceeded,
    DevPersistenceService,
    DevPlatformAllowance,
    DevRateLimitExceeded,
)
from dev_health_ops.models.dev_persistence import (
    DevConversation,
    DevMessage,
    DevRun,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_TABLES = tables_of(
    Organization,
    User,
    DevConversation,
    DevMessage,
    DevRun,
)

pytestmark = pytest.mark.skipif(
    not os.getenv(_POSTGRES_URI_ENV),
    reason=f"requires {_POSTGRES_URI_ENV}",
)


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
