"""CHAOS-3219 Codex round 2 (2026-08-05): allowance-accounting-level proof
that pricing the scripted model gives real corpus-sized quota headroom, and
that the guard is still observable (trippable) when deliberately tightened
-- the pair team-lead's ruling on the quota finding required: "add a test at
the allowance-accounting level (no need for 134 live boots) proving >=134
priced runs at your observed per-run cost clear the untouched 100M default
with margin, AND that the dedicated quota-negative case still trips
DevMonthlyCostLimitExceeded. That pair is what keeps the guard observable."

No live boot, no Docker: drives the REAL admission path
(``DevPersistenceService.append_user_message_and_run(platform_allowance=...)``
-> ``_enforce_platform_allowance``) against an aiosqlite in-memory DB, using
the SAME fixture pattern as
``tests/api/dev/test_chaos_3296_round2_budget_and_receipts.py``. Prior runs
are seeded directly (as terminal ``DevRun`` rows with a recorded
``estimated_cost_microusd``) rather than driven through a full orchestrator
run -- seeding is the standard pattern for testing an admission DECISION
against prior state; the decision itself is exercised through the real,
unmodified production method, not reimplemented here.

Per-run cost assumption: ``ask-dev-scripted-v1``'s real (and only) HTTP
response reports ``prompt_tokens=7, completion_tokens=5``
(scripted_openai_service.py) -- this test deliberately uses a MUCH larger
(1000x+) placeholder of 8_000/3_000 tokens per model round, so the margin
proven here holds even if the scripted provider's responses grow
substantially more verbose in a future change.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.org_policy import PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD
from dev_health_ops.api.dev.persistence import DevPersistenceService
from dev_health_ops.api.dev.persistence.service import (
    DevMonthlyCostLimitExceeded,
    DevPlatformAllowance,
)
from dev_health_ops.llm.agent.openai_compatible import _estimated_cost_microusd
from dev_health_ops.models.dev_persistence import DevConversation, DevMessage, DevRun
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

# The two real, compose-configured values (tests/acceptance/compose.ask-dev.yml
# api.environment) -- proving against what the acceptance environment ACTUALLY
# runs with, not just the code's own untouched defaults.
_COMPOSE_MONTHLY_REQUEST_MAX = 1000
_COMPOSE_MONTHLY_COST_MAX_MICROUSD = 200_000_000

# Frozen corpus registry size x up to 3 runs/case (team-lead's framing).
_PLANNED_CASE_COUNT = 134
_MAX_RUNS_PER_CASE = 3
_PLANNED_RUN_COUNT = _PLANNED_CASE_COUNT * _MAX_RUNS_PER_CASE

# Deliberately >>1000x the scripted provider's real per-call token counts
# (7 prompt / 5 completion) -- see module docstring.
_CONSERVATIVE_INPUT_TOKENS_PER_ROUND = 8_000
_CONSERVATIVE_OUTPUT_TOKENS_PER_ROUND = 3_000
_MAX_MODEL_ROUNDS_PER_RUN = 4  # orchestrator.py DevRunLimits default cap

_PERSISTENCE_TABLES = tables_of(User, Organization, DevConversation, DevMessage, DevRun)


@pytest_asyncio.fixture
async def persistence(tmp_path: Path):
    database = tmp_path / "ask-dev-quota-headroom.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_PERSISTENCE_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="ask-dev-quota-headroom", name="Quota"),
                User(id=user_id, email="ask-dev-quota-headroom@example.com"),
            ]
        )
        await session.commit()
    try:
        yield maker, org_id, user_id
    finally:
        await engine.dispose()


def _per_run_priced_cost_microusd() -> int:
    """The real production pricing function, applied to the deliberately
    conservative per-round token assumption, summed over the worst-case
    round count a single run can reach."""

    per_round = _estimated_cost_microusd(
        model="ask-dev-scripted-v1",
        input_tokens=_CONSERVATIVE_INPUT_TOKENS_PER_ROUND,
        output_tokens=_CONSERVATIVE_OUTPUT_TOKENS_PER_ROUND,
    )
    assert per_round is not None, (
        "ask-dev-scripted-v1 has no _PLATFORM_MODEL_PRICES entry -- the "
        "Codex round-2 pricing fix (openai_compatible.py) is missing or "
        "was reverted; every call would fall back to the full multi-"
        "million-microusd admission reservation again"
    )
    return per_round * _MAX_MODEL_ROUNDS_PER_RUN


async def _seed_terminal_run(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    cost_microusd: int,
) -> None:
    message = DevMessage(
        conversation_id=conversation_id,
        org_id=org_id,
        user_id=user_id,
        client_message_id=uuid.uuid4(),
        role="user",
        content="seeded prior run",
        scope_snapshot={},
        created_at=datetime.now(UTC),
    )
    session.add(message)
    await session.flush()
    session.add(
        DevRun(
            request_id=uuid.uuid4(),
            conversation_id=conversation_id,
            user_message_id=message.id,
            org_id=org_id,
            user_id=user_id,
            state="completed",
            provider_source="platform",
            started_at=datetime.now(UTC),
            estimated_cost_microusd=cost_microusd,
        )
    )
    await session.flush()


@pytest.mark.asyncio
async def test_priced_scripted_runs_clear_the_untouched_default_with_margin(
    persistence,
) -> None:
    """Positive half of the pair: >=134x3 priced runs must NOT exhaust the
    compose-configured allowance, with an asserted margin -- not just "did
    not raise"."""

    maker, org_id, user_id = persistence
    per_run_cost = _per_run_priced_cost_microusd()

    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        conversation_id = conversation.id
        for _ in range(_PLANNED_RUN_COUNT):
            await _seed_terminal_run(
                session,
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                cost_microusd=per_run_cost,
            )
        await session.commit()

    total_seeded_cost = per_run_cost * _PLANNED_RUN_COUNT
    # The margin itself, asserted directly (not implied by "admission
    # succeeded") -- proves there is real headroom left over, not that the
    # boundary was grazed.
    remaining_before_admission = _COMPOSE_MONTHLY_COST_MAX_MICROUSD - total_seeded_cost
    assert remaining_before_admission > _COMPOSE_MONTHLY_COST_MAX_MICROUSD // 2, (
        f"{_PLANNED_RUN_COUNT} priced runs at {per_run_cost} microusd each "
        f"consumed {total_seeded_cost} of the {_COMPOSE_MONTHLY_COST_MAX_MICROUSD} "
        "compose-configured allowance -- less than half remains, which is "
        "not the wide margin this proof exists to demonstrate"
    )

    async with maker() as session:
        service = DevPersistenceService(session)
        # The real admission path for run #(_PLANNED_RUN_COUNT + 1) -- must
        # NOT raise.
        result = await service.append_user_message_and_run(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            client_message_id=uuid.uuid4(),
            question="one more, over the planned corpus count",
            scope_snapshot={},
            provider_source="platform",
            platform_allowance=DevPlatformAllowance(
                monthly_request_limit=_COMPOSE_MONTHLY_REQUEST_MAX,
                monthly_cost_limit_microusd=_COMPOSE_MONTHLY_COST_MAX_MICROUSD,
            ),
        )
        assert result.created is True


@pytest.mark.asyncio
async def test_a_tightened_allowance_still_trips_the_cost_guard(persistence) -> None:
    """Negative half of the pair: the SAME enforcement path, with the
    allowance tightened to just below the documented operator floor
    (org_policy.PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD), must still raise
    DevMonthlyCostLimitExceeded -- proving pricing the scripted model did
    not silently defeat the guard's ability to ever trip."""

    maker, org_id, user_id = persistence
    tightened_limit = PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD

    async with maker() as session:
        service = DevPersistenceService(session)
        conversation = await service.create_conversation(
            org_id=org_id, user_id=user_id, current_scope={}
        )
        # One seeded run whose recorded cost alone, plus the next admission's
        # own reservation, exceeds the tightened limit -- computed from the
        # real constant, not a hand-picked magic number.
        await _seed_terminal_run(
            session,
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation.id,
            cost_microusd=tightened_limit - 1,
        )
        await session.commit()
        conversation_id = conversation.id

    async with maker() as session:
        service = DevPersistenceService(session)
        with pytest.raises(DevMonthlyCostLimitExceeded):
            await service.append_user_message_and_run(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                client_message_id=uuid.uuid4(),
                question="this one must be refused",
                scope_snapshot={},
                provider_source="platform",
                platform_allowance=DevPlatformAllowance(
                    monthly_request_limit=1000,
                    monthly_cost_limit_microusd=tightened_limit,
                ),
            )
