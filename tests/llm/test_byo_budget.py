from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.llm.agent.contracts import (
    AgentMessage,
    AgentMessageRole,
    AgentToolDefinition,
)
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.openai_compatible import OpenAICompatibleAgentProvider
from dev_health_ops.llm.budget import (
    BYOBudgetAccountingError,
    BYOBudgetExceeded,
    attach_agent_budget_guard,
    attach_llm_budget_guard,
    cost_micro_usd,
    get_budget_status,
    guard_byo_call,
    set_budget_limit,
)
from dev_health_ops.llm.providers.openai import OpenAIProvider
from dev_health_ops.models.git import Base
from dev_health_ops.models.llm_budget import BYOLLMBudgetReservation
from dev_health_ops.models.settings import Setting
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'budget.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(Organization, Setting, BYOLLMBudgetReservation),
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _configure(session_maker, org_id: str, limit: int) -> None:
    async with session_maker() as session:
        session.add(
            Organization(
                id=uuid.UUID(org_id),
                slug=f"budget-{org_id[:8]}",
                name="Budget Test Organization",
                tier="team",
            )
        )
        await set_budget_limit(SettingsService(session, org_id), limit)
        await session.commit()


async def _seed_org_without_budget(session_maker, org_id: str) -> None:
    async with session_maker() as session:
        session.add(
            Organization(
                id=uuid.UUID(org_id),
                slug=f"budget-{org_id[:8]}",
                name="Budget Test Organization",
                tier="team",
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_known_price_is_integer_micro_usd_and_unknown_is_not_zero():
    assert (
        cost_micro_usd(
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            cached_input_tokens=0,
        )
        == 2_250_000
    )
    assert (
        cost_micro_usd(
            provider="openai",
            model="gpt-5-mini-2025-08-07",
            base_url="https://api.openai.com/v1",
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            cached_input_tokens=1_000_000,
        )
        == 2_025_000
    )
    assert (
        cost_micro_usd(
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
            input_tokens=10,
            output_tokens=10,
            cached_input_tokens=None,
        )
        is None
    )
    assert (
        cost_micro_usd(
            provider="openai",
            model="customer-model",
            base_url="https://gateway.example/v1",
            input_tokens=10,
            output_tokens=10,
            cached_input_tokens=0,
        )
        is None
    )


@pytest.mark.asyncio
async def test_unknown_pricing_reports_unavailable_never_zero(session_maker):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)
    async with session_maker() as session:
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="private-model",
            base_url="https://gateway.example/v1",
        )
    assert status.used_micro_usd is None
    assert status.remaining_micro_usd is None
    assert status.enforcement_available is False
    assert status.reason == "pricing_unavailable"


@pytest.mark.asyncio
async def test_configured_budget_rejects_unknown_pricing_before_provider_execution(
    session_maker,
):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)
    calls = 0

    async def invoke() -> tuple[int, int, int]:
        nonlocal calls
        calls += 1
        return 1, 1, 0

    async with session_maker() as session:
        with pytest.raises(BYOBudgetAccountingError):
            await guard_byo_call(
                session=session,
                org_id=org_id,
                provider="openai",
                model="private-model",
                base_url="https://gateway.example/v1",
                idempotency_key="unknown-price",
                maximum_input_tokens=1,
                maximum_output_tokens=1,
                invoke=invoke,
                usage=lambda value: value,
            )

    assert calls == 0


@pytest.mark.asyncio
async def test_unconfigured_budget_preserves_unknown_price_provider_execution(
    session_maker,
):
    org_id = str(uuid.uuid4())
    await _seed_org_without_budget(session_maker, org_id)

    async with session_maker() as session:

        async def invoke() -> tuple[int, int, int]:
            assert session.in_transaction() is False
            return 1, 1, 0

        result, billed = await guard_byo_call(
            session=session,
            org_id=org_id,
            provider="openai",
            model="private-model",
            base_url="https://gateway.example/v1",
            idempotency_key="unconfigured-unknown-price",
            maximum_input_tokens=1,
            maximum_output_tokens=1,
            invoke=invoke,
            usage=lambda value: value,
        )

    assert result == (1, 1, 0)
    assert billed is None


@pytest.mark.asyncio
async def test_configured_budget_rejects_unpriceable_batch_before_execution(
    session_maker, monkeypatch
):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)
    calls = 0

    @asynccontextmanager
    async def budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.budget.get_postgres_session", budget_session
    )

    class BatchProvider:
        async def complete(self, prompt: str):
            raise AssertionError(f"unexpected synchronous call: {prompt}")

        async def submit_batch(self, items):
            nonlocal calls
            calls += 1
            return items

    provider = attach_llm_budget_guard(
        BatchProvider(),  # type: ignore[arg-type]
        org_id=org_id,
        provider="openai",
        model="gpt-5-mini",
        base_url=None,
    )
    with pytest.raises(BYOBudgetAccountingError):
        await provider.submit_batch([])  # type: ignore[attr-defined]

    assert calls == 0


@pytest.mark.asyncio
async def test_admission_records_usage_idempotently_and_rejects_exhaustion(
    session_maker,
):
    org_id = str(uuid.uuid4())
    one_call = cost_micro_usd(
        provider="openai",
        model="gpt-5-mini",
        base_url=None,
        input_tokens=4,
        output_tokens=2,
        cached_input_tokens=0,
    )
    assert one_call is not None
    await _configure(session_maker, org_id, one_call)

    calls = 0

    async def invoke() -> tuple[int, int, int]:
        nonlocal calls
        calls += 1
        return 4, 2, 0

    async with session_maker() as session:
        await guard_byo_call(
            session=session,
            org_id=org_id,
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
            idempotency_key="call-1",
            maximum_input_tokens=4,
            maximum_output_tokens=2,
            invoke=invoke,
            usage=lambda value: value,
        )

    # A replay cannot execute the provider again because doing so would incur a
    # second billable call under the same accounting identity.
    async with session_maker() as session:
        with pytest.raises(BYOBudgetAccountingError):
            await guard_byo_call(
                session=session,
                org_id=org_id,
                provider="openai",
                model="gpt-5-mini",
                base_url=None,
                idempotency_key="call-1",
                maximum_input_tokens=0,
                maximum_output_tokens=0,
                invoke=invoke,
                usage=lambda value: value,
            )
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )
    assert calls == 1
    assert status.used_micro_usd == one_call
    assert status.remaining_micro_usd == 0

    async with session_maker() as session:
        with pytest.raises(BYOBudgetExceeded):
            await guard_byo_call(
                session=session,
                org_id=org_id,
                provider="openai",
                model="gpt-5-mini",
                base_url=None,
                idempotency_key="call-2",
                maximum_input_tokens=4,
                maximum_output_tokens=2,
                invoke=invoke,
                usage=lambda value: value,
            )
    assert calls == 1


@pytest.mark.asyncio
async def test_concurrent_admissions_cannot_both_spend_one_call_budget(session_maker):
    org_id = str(uuid.uuid4())
    one_call = cost_micro_usd(
        provider="openai",
        model="gpt-5-mini",
        base_url=None,
        input_tokens=4,
        output_tokens=2,
        cached_input_tokens=0,
    )
    assert one_call is not None
    await _configure(session_maker, org_id, one_call)
    entered = 0

    async def attempt(key: str) -> str:
        async with session_maker() as session:

            async def invoke() -> tuple[int, int, int]:
                nonlocal entered
                entered += 1
                await asyncio.sleep(0)
                return 4, 2, 0

            try:
                await guard_byo_call(
                    session=session,
                    org_id=org_id,
                    provider="openai",
                    model="gpt-5-mini",
                    base_url=None,
                    idempotency_key=key,
                    maximum_input_tokens=4,
                    maximum_output_tokens=2,
                    invoke=invoke,
                    usage=lambda value: value,
                )
            except BYOBudgetExceeded:
                return "rejected"
            return "admitted"

    outcomes = await asyncio.gather(attempt("one"), attempt("two"))
    assert sorted(outcomes) == ["admitted", "rejected"]
    assert entered == 1


@pytest.mark.asyncio
async def test_missing_reported_usage_marks_window_unavailable(session_maker):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)
    async with session_maker() as session:
        with pytest.raises(BYOBudgetAccountingError):
            await guard_byo_call(
                session=session,
                org_id=org_id,
                provider="openai",
                model="gpt-5-mini",
                base_url=None,
                idempotency_key="missing-usage",
                maximum_input_tokens=1,
                maximum_output_tokens=1,
                invoke=lambda: asyncio.sleep(0, result=(None, None, None)),
                usage=lambda value: value,
            )
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )
    assert status.used_micro_usd is None
    assert status.remaining_micro_usd is None
    assert status.enforcement_available is False
    assert status.reason == "usage_unavailable"

    calls = 0

    async def invoke() -> tuple[int, int, int]:
        nonlocal calls
        calls += 1
        return 1, 1, 0

    async with session_maker() as session:
        with pytest.raises(BYOBudgetAccountingError):
            await guard_byo_call(
                session=session,
                org_id=org_id,
                provider="openai",
                model="gpt-5-mini",
                base_url=None,
                idempotency_key="after-missing-usage",
                maximum_input_tokens=1,
                maximum_output_tokens=1,
                invoke=invoke,
                usage=lambda value: value,
            )
    assert calls == 0


@pytest.mark.asyncio
async def test_provider_execution_occurs_after_reservation_transaction_commits(
    session_maker,
):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)

    async with session_maker() as session:

        async def invoke() -> tuple[int, int, int]:
            assert session.in_transaction() is False
            async with session_maker() as observer:
                rows = await observer.execute(select(BYOLLMBudgetReservation))
                reservation = rows.scalar_one()
                assert reservation.status == "reserved"
                assert reservation.actual_micro_usd is None
            return 4, 2, 0

        await guard_byo_call(
            session=session,
            org_id=org_id,
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
            idempotency_key="transaction-boundary",
            maximum_input_tokens=4,
            maximum_output_tokens=2,
            invoke=invoke,
            usage=lambda value: value,
        )

    async with session_maker() as session:
        rows = await session.execute(select(BYOLLMBudgetReservation))
        reservation = rows.scalar_one()
        assert reservation.status == "succeeded"
        assert reservation.reserved_micro_usd == 5
        assert reservation.actual_micro_usd == 5
        assert reservation.reconciled_at is not None


@pytest.mark.asyncio
async def test_window_resets_at_next_utc_month_and_old_usage_does_not_carry(
    session_maker,
):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)
    async with session_maker() as session:
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
            now=datetime(2026, 12, 31, 23, 59, tzinfo=UTC),
        )
    assert status.used_micro_usd == 0
    assert status.reset_at == datetime(2027, 1, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_failed_call_counts_usage_when_provider_reports_it(session_maker):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)

    class ReportedFailure(RuntimeError):
        usage = {
            "prompt_tokens": 4,
            "completion_tokens": 2,
            "prompt_tokens_details": {"cached_tokens": 0},
        }

    async def invoke():
        raise ReportedFailure("provider failed")

    async with session_maker() as session:
        with pytest.raises(ReportedFailure):
            await guard_byo_call(
                session=session,
                org_id=org_id,
                provider="openai",
                model="gpt-5-mini",
                base_url=None,
                idempotency_key="failed-call",
                maximum_input_tokens=4,
                maximum_output_tokens=2,
                invoke=invoke,
                usage=lambda value: value,
            )

    async with session_maker() as session:
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )
    assert status.used_micro_usd == 5


@pytest.mark.asyncio
async def test_pre_dispatch_cancellation_voids_reservation_without_poisoning_window(
    session_maker,
):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)

    async def invoke():
        raise AgentProviderError(
            AgentProviderErrorCode.CANCELLED, provider_dispatched=False
        )

    async with session_maker() as session:
        with pytest.raises(AgentProviderError):
            await guard_byo_call(
                session=session,
                org_id=org_id,
                provider="openai",
                model="gpt-5-mini",
                base_url=None,
                idempotency_key="cancelled-before-dispatch",
                maximum_input_tokens=4,
                maximum_output_tokens=2,
                invoke=invoke,
                usage=lambda value: value,
            )

    async with session_maker() as session:
        row = (await session.execute(select(BYOLLMBudgetReservation))).scalar_one()
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )

    assert row.status == "voided"
    assert row.actual_micro_usd == 0
    assert (row.input_tokens, row.output_tokens, row.cached_input_tokens) == (0, 0, 0)
    assert status.reason == "available"
    assert status.used_micro_usd == 0
    assert status.remaining_micro_usd == 50_000


@pytest.mark.asyncio
async def test_unreconciled_reservation_remains_counted_conservatively(session_maker):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 50_000)
    async with session_maker() as session:
        session.add(
            BYOLLMBudgetReservation(
                org_id=uuid.UUID(org_id),
                window_start=datetime.now(UTC).replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                ),
                idempotency_key="orphaned",
                provider="openai",
                model="gpt-5-mini",
                reserved_micro_usd=20_000,
                status="reserved",
                pricing_version="test",
                created_at=datetime(2020, 1, 1, tzinfo=UTC),
            )
        )
        await session.commit()

        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )

    assert status.used_micro_usd == 20_000
    assert status.remaining_micro_usd == 30_000


@pytest.mark.asyncio
async def test_openai_retry_attempts_are_independently_admitted_and_accounted(
    session_maker, monkeypatch
):
    org_id = str(uuid.uuid4())
    one_attempt = cost_micro_usd(
        provider="openai",
        model="gpt-5-mini",
        base_url=None,
        input_tokens=4,
        output_tokens=4096,
        cached_input_tokens=0,
    )
    assert one_attempt == 8193
    budget_limit = 100_000
    await _configure(session_maker, org_id, budget_limit)

    @asynccontextmanager
    async def budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.budget.get_postgres_session", budget_session
    )

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("dev_health_ops.llm.providers.openai.asyncio.sleep", no_sleep)

    responses = [
        SimpleNamespace(
            output_text="",
            output=[],
            incomplete_details=SimpleNamespace(reason="max_output_tokens"),
            usage=SimpleNamespace(
                input_tokens=4,
                output_tokens=4096,
                input_tokens_details=SimpleNamespace(cached_tokens=0),
            ),
        ),
        SimpleNamespace(
            output_text='{"ok": true}',
            output=[],
            incomplete_details=None,
            usage=SimpleNamespace(
                input_tokens=4,
                output_tokens=4096,
                input_tokens_details=SimpleNamespace(cached_tokens=0),
            ),
        ),
    ]

    class Responses:
        def __init__(self):
            self.calls: list[dict] = []

        async def create(self, **kwargs):
            self.calls.append(kwargs)
            return responses.pop(0)

    client = SimpleNamespace(responses=Responses())
    provider = OpenAIProvider(api_key="unused", model="gpt-5-mini")
    provider._impl._client = client
    guarded = attach_llm_budget_guard(
        provider,
        org_id=org_id,
        provider="openai",
        model="gpt-5-mini",
        base_url=None,
    )

    result = await guarded.complete("test")

    async with session_maker() as session:
        rows = list((await session.execute(select(BYOLLMBudgetReservation))).scalars())
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )

    assert result.text == '{"ok": true}'
    assert len(client.responses.calls) == 2
    assert len(rows) == 2
    assert all(row.status == "succeeded" for row in rows)
    assert sum(row.actual_micro_usd or 0 for row in rows) == one_attempt * 2
    assert status.used_micro_usd == one_attempt * 2
    assert status.remaining_micro_usd == budget_limit - (one_attempt * 2)


@pytest.mark.asyncio
async def test_openai_retry_is_rejected_before_second_network_attempt(
    session_maker, monkeypatch
):
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 20_000)

    @asynccontextmanager
    async def budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.budget.get_postgres_session", budget_session
    )

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("dev_health_ops.llm.providers.openai.asyncio.sleep", no_sleep)

    response = SimpleNamespace(
        output_text="",
        output=[],
        incomplete_details=SimpleNamespace(reason="max_output_tokens"),
        usage=SimpleNamespace(
            input_tokens=4,
            output_tokens=4096,
            input_tokens_details=SimpleNamespace(cached_tokens=0),
        ),
    )

    class Responses:
        def __init__(self):
            self.calls = 0

        async def create(self, **_kwargs):
            self.calls += 1
            return response

    client = SimpleNamespace(responses=Responses())
    provider = OpenAIProvider(api_key="unused", model="gpt-5-mini")
    provider._impl._client = client
    guarded = attach_llm_budget_guard(
        provider,
        org_id=org_id,
        provider="openai",
        model="gpt-5-mini",
        base_url=None,
    )

    with pytest.raises(BYOBudgetExceeded):
        await guarded.complete("test")

    assert client.responses.calls == 1


@pytest.mark.asyncio
async def test_output_exhaustion_reconciles_with_actual_cost_and_leaves_budget_admissible(
    session_maker, monkeypatch
):
    """CHAOS-3285 integration reproduction through attach_agent_budget_guard.

    Before the fix: OUTPUT_EXHAUSTED was raised by the adapter before
    response.usage was ever read, so the exception reaching guard_byo_call's
    exception handler carried no usage. _reconcile_reservation then marked
    the reservation "usage_unavailable" (not "failed" with a real cost) --
    and any reservation in that state poisons the whole calendar-month
    window (get_budget_status short-circuits to enforcement_available=False
    for every subsequent call), so the very next request is rejected with
    BUDGET_UNAVAILABLE *without ever dispatching to the provider*.

    After the fix: the adapter attaches the real billed usage to the raised
    AgentProviderError, so the reservation reconciles as an ordinary FAILED
    call with its actual token cost, and the budget window stays admissible
    -- the next request still dispatches.
    """
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 1_000_000)

    @asynccontextmanager
    async def budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.budget.get_postgres_session", budget_session
    )

    # Reviewer's exact reproduction: a parseable finish_reason="length"
    # response reporting 40 input / 256 output / 240 reasoning tokens.
    exhausted_response = SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="length",
                message=SimpleNamespace(content="", tool_calls=[]),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=40,
            completion_tokens=256,
            prompt_tokens_details=SimpleNamespace(cached_tokens=0),
            completion_tokens_details=SimpleNamespace(reasoning_tokens=240),
        ),
    )
    ok_call = SimpleNamespace(
        id="call-1", function=SimpleNamespace(name="lookup", arguments="{}")
    )
    ok_response = SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(content=None, tool_calls=[ok_call]),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=5,
            completion_tokens=5,
            prompt_tokens_details=SimpleNamespace(cached_tokens=0),
            completion_tokens_details=SimpleNamespace(reasoning_tokens=None),
        ),
    )

    class Completions:
        def __init__(self, responses):
            self.responses = responses
            self.calls = 0

        async def create(self, **_kwargs):
            self.calls += 1
            return self.responses.pop(0)

    completions = Completions([exhausted_response, ok_response])
    client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    provider = OpenAICompatibleAgentProvider(
        api_key="unused", model="gpt-5-mini", client=client
    )
    async with session_maker() as unused_session:
        guarded = attach_agent_budget_guard(
            provider,
            session=unused_session,
            org_id=org_id,
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )

    tools = [AgentToolDefinition("lookup", "Lookup", {"type": "object"})]

    with pytest.raises(AgentProviderError) as caught:
        await guarded.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            tools,
            {"type": "object"},
            1,
            256,
        )
    assert caught.value.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED

    expected_cost = cost_micro_usd(
        provider="openai",
        model="gpt-5-mini",
        base_url=None,
        input_tokens=40,
        output_tokens=256,
        cached_input_tokens=0,
    )
    assert expected_cost is not None

    async with session_maker() as session:
        rows = list((await session.execute(select(BYOLLMBudgetReservation))).scalars())
    assert len(rows) == 1
    assert rows[0].status == "failed"
    assert rows[0].actual_micro_usd == expected_cost
    assert (rows[0].input_tokens, rows[0].output_tokens) == (40, 256)

    async with session_maker() as session:
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )
    assert status.reason != "usage_unavailable"
    assert status.enforcement_available is True
    assert status.used_micro_usd == expected_cost

    # The window must remain admissible: the next request still dispatches
    # to the provider rather than being rejected pre-dispatch.
    result = await guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "question")],
        tools,
        {"type": "object"},
        1,
        256,
    )
    assert completions.calls == 2
    assert result.decision.tool_id == "lookup"

    async with session_maker() as session:
        rows = list((await session.execute(select(BYOLLMBudgetReservation))).scalars())
    assert len(rows) == 2
    assert sorted(row.status for row in rows) == ["failed", "succeeded"]


@pytest.mark.asyncio
async def test_zero_token_exhaustion_holds_reservation_instead_of_reconciling_as_free(
    session_maker, monkeypatch
):
    """CHAOS-3285 follow-up integration reproduction.

    A provider can report finish_reason="length" with a usage object whose
    prompt_tokens/completion_tokens are both exactly 0 (or omit ``usage``
    entirely, which normalizes to the same shape) -- a valid completion can
    never actually consume zero of both, so this is unreported usage, not a
    free call. Before this follow-up fix, the adapter still attached that
    all-zero usage to the raised error, and guard_byo_call reconciled the
    reservation as status="failed" with actual_micro_usd=0: a genuinely
    unknown cost silently became a real $0 charge, dropping it from BYO
    budget accounting entirely.

    After the fix, usage is withheld from the error in this case, so the
    reservation reconciles the same conservative way the codebase already
    handles any other genuinely-unreported-usage failure (pre-existing
    behavior, unrelated to and not reintroducing the window-poisoning bug
    the OTHER CHAOS-3285 integration test above guards against for the
    reported-usage case): held as "usage_unavailable", never as a $0 charge.
    """
    org_id = str(uuid.uuid4())
    await _configure(session_maker, org_id, 1_000_000)

    @asynccontextmanager
    async def budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.budget.get_postgres_session", budget_session
    )

    exhausted_response = SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="length",
                message=SimpleNamespace(content="", tool_calls=[]),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=0,
            completion_tokens=0,
            prompt_tokens_details=SimpleNamespace(cached_tokens=0),
            completion_tokens_details=SimpleNamespace(reasoning_tokens=0),
        ),
    )

    class Completions:
        def __init__(self, responses):
            self.responses = responses
            self.calls = 0

        async def create(self, **_kwargs):
            self.calls += 1
            return self.responses.pop(0)

    completions = Completions([exhausted_response])
    client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    provider = OpenAICompatibleAgentProvider(
        api_key="unused", model="gpt-5-mini", client=client
    )
    async with session_maker() as unused_session:
        guarded = attach_agent_budget_guard(
            provider,
            session=unused_session,
            org_id=org_id,
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )

    tools = [AgentToolDefinition("lookup", "Lookup", {"type": "object"})]

    with pytest.raises(AgentProviderError) as caught:
        await guarded.decide(
            [AgentMessage(AgentMessageRole.USER, "question")],
            tools,
            {"type": "object"},
            1,
            256,
        )
    assert caught.value.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED

    async with session_maker() as session:
        rows = list((await session.execute(select(BYOLLMBudgetReservation))).scalars())
    assert len(rows) == 1
    assert rows[0].status == "usage_unavailable"
    assert rows[0].actual_micro_usd is None

    async with session_maker() as session:
        status = await get_budget_status(
            SettingsService(session, org_id),
            provider="openai",
            model="gpt-5-mini",
            base_url=None,
        )
    assert status.reason == "usage_unavailable"
    assert status.enforcement_available is False
