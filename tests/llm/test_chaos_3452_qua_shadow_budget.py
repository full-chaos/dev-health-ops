"""CHAOS-3452: the isolated QUA shadow quota never touches the live BYO
budget pool, in either direction.

RED-first coverage:
(1) structural isolation -- a shadow call NEVER writes into
    ``byo_llm_budget_reservations`` and a live call NEVER writes into
    ``dev_qua_shadow_budget_reservations``;
(2) exhausting the live org's configured BYO ceiling does not block a
    concurrent shadow call, and exhausting the isolated shadow quota does
    not block a concurrent live call;
(3) mutation-style proof that (1) actually discriminates: monkeypatching
    the shadow guard to point at the LIVE reservation table makes a shadow
    call write into ``byo_llm_budget_reservations`` -- i.e. the isolation
    property is not vacuously true, and a future accidental pool-merge
    would be caught here.
"""

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.configuration import SettingsService
from dev_health_ops.llm import qua_shadow_budget
from dev_health_ops.llm.agent.contracts import (
    AgentDecisionResult,
    AgentFinalAnswer,
    AgentMessage,
    AgentMessageRole,
    AgentUsage,
)
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.budget import (
    attach_agent_budget_guard,
    cost_micro_usd,
    set_budget_limit,
)
from dev_health_ops.llm.qua_shadow_budget import (
    DEFAULT_QUA_SHADOW_MAX_MICRO_USD,
    QUA_SHADOW_BUDGET_ENV_KEY,
    attach_qua_shadow_budget_guard,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.llm_budget import (
    BYOLLMBudgetReservation,
    QUAShadowBudgetReservation,
)
from dev_health_ops.models.settings import Setting
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

_PROVIDER = "openai"
_MODEL = "gpt-5-mini"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'qua_shadow.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(
                    Organization,
                    Setting,
                    BYOLLMBudgetReservation,
                    QUAShadowBudgetReservation,
                ),
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_org(session_maker, org_id: str) -> None:
    async with session_maker() as session:
        session.add(
            Organization(
                id=uuid.UUID(org_id),
                slug=f"qua-shadow-{org_id[:8]}",
                name="QUA Shadow Test Organization",
                tier="team",
            )
        )
        await session.commit()


class _FakeProvider:
    """A minimal ``AgentLLMProvider``-shaped fake: `.decide()` returns a
    fixed, cheap, well-formed result every call -- enough for the budget
    guard's usage extraction and reconciliation, nothing more."""

    def __init__(self) -> None:
        self.calls = 0

    async def decide(
        self,
        messages,
        tools,
        response_schema,
        timeout_seconds,
        max_output_tokens,
        signal=None,
    ) -> AgentDecisionResult:
        self.calls += 1
        return AgentDecisionResult(
            decision=AgentFinalAnswer(value={"ok": True}),
            usage=AgentUsage(input_tokens=5, output_tokens=5, cached_input_tokens=0),
            latency_ms=1,
            provider_fingerprint="fake",
            model_fingerprint="fake",
        )

    async def aclose(self) -> None:
        return None


class _RaisingProvider:
    """A shadow provider whose `.decide()` always raises a fixed,
    caller-supplied error -- used to exercise the exception-reconciliation
    path directly."""

    def __init__(self, error: AgentProviderError) -> None:
        self._error = error
        self.calls = 0

    async def decide(
        self,
        messages,
        tools,
        response_schema,
        timeout_seconds,
        max_output_tokens,
        signal=None,
    ) -> AgentDecisionResult:
        self.calls += 1
        raise self._error

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_shadow_call_never_writes_the_live_reservation_table(
    session_maker, monkeypatch
):
    """Structural isolation, direction 1: a shadow call reserves/reconciles
    ONLY in dev_qua_shadow_budget_reservations."""

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )

    provider = _FakeProvider()
    guarded = attach_qua_shadow_budget_guard(
        provider, org_id=org_id, provider=_PROVIDER, model=_MODEL, base_url=None
    )
    result = await guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "question")], (), {"type": "object"}, 1, 64
    )
    assert result.decision.value == {"ok": True}
    assert provider.calls == 1

    async with session_maker() as session:
        live_rows = list(
            (await session.execute(select(BYOLLMBudgetReservation))).scalars()
        )
        shadow_rows = list(
            (await session.execute(select(QUAShadowBudgetReservation))).scalars()
        )
    assert live_rows == []
    assert len(shadow_rows) == 1
    assert shadow_rows[0].status == "succeeded"


@pytest.mark.asyncio
async def test_live_call_never_writes_the_shadow_reservation_table(
    session_maker, monkeypatch
):
    """Structural isolation, direction 2: a live call reserves/reconciles
    ONLY in byo_llm_budget_reservations."""

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)
    async with session_maker() as session:
        await set_budget_limit(SettingsService(session, org_id), 1_000_000)
        await session.commit()

    @asynccontextmanager
    async def live_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.budget.get_postgres_session", live_budget_session
    )

    provider = _FakeProvider()
    async with session_maker() as unused_session:
        guarded = attach_agent_budget_guard(
            provider,
            session=unused_session,
            org_id=org_id,
            provider=_PROVIDER,
            model=_MODEL,
            base_url=None,
        )
    result = await guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "question")], (), {"type": "object"}, 1, 64
    )
    assert result.decision.value == {"ok": True}

    async with session_maker() as session:
        live_rows = list(
            (await session.execute(select(BYOLLMBudgetReservation))).scalars()
        )
        shadow_rows = list(
            (await session.execute(select(QUAShadowBudgetReservation))).scalars()
        )
    assert len(live_rows) == 1
    assert shadow_rows == []


@pytest.mark.asyncio
async def test_exhausting_the_live_budget_does_not_block_a_concurrent_shadow_call(
    session_maker, monkeypatch
):
    """The live org's ceiling is configured to zero -- a live call is
    rejected outright -- but a shadow call for the SAME org, in the SAME
    window, still succeeds: it draws from its own separate, unexhausted
    pool, entirely unaware the live pool is exhausted."""

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)
    async with session_maker() as session:
        await set_budget_limit(SettingsService(session, org_id), 0)
        await session.commit()

    @asynccontextmanager
    async def live_budget_session():
        async with session_maker() as session:
            yield session

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.budget.get_postgres_session", live_budget_session
    )
    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )

    live_provider = _FakeProvider()
    async with session_maker() as unused_session:
        live_guarded = attach_agent_budget_guard(
            live_provider,
            session=unused_session,
            org_id=org_id,
            provider=_PROVIDER,
            model=_MODEL,
            base_url=None,
        )
    with pytest.raises(AgentProviderError) as live_caught:
        await live_guarded.decide(
            [AgentMessage(AgentMessageRole.USER, "q1")], (), {"type": "object"}, 1, 64
        )
    assert live_caught.value.code is AgentProviderErrorCode.BUDGET_EXHAUSTED
    assert live_provider.calls == 0

    # A shadow call for the SAME org, SAME window, is entirely unaffected.
    shadow_provider = _FakeProvider()
    shadow_guarded = attach_qua_shadow_budget_guard(
        shadow_provider,
        org_id=org_id,
        provider=_PROVIDER,
        model=_MODEL,
        base_url=None,
    )
    shadow_result = await shadow_guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "q2")], (), {"type": "object"}, 1, 64
    )
    assert shadow_result.decision.value == {"ok": True}
    assert shadow_provider.calls == 1


@pytest.mark.asyncio
async def test_exhausting_the_shadow_quota_does_not_block_a_concurrent_live_call(
    session_maker, monkeypatch
):
    """The mirror image: an isolated shadow quota configured to zero
    rejects a shadow call outright, but the live call for the SAME org,
    SAME window, still succeeds against its own, generously-configured
    ceiling -- entirely unaware the shadow pool is exhausted."""

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)
    async with session_maker() as session:
        await set_budget_limit(SettingsService(session, org_id), 1_000_000)
        await session.commit()
    monkeypatch.setenv(QUA_SHADOW_BUDGET_ENV_KEY, "0")

    @asynccontextmanager
    async def live_budget_session():
        async with session_maker() as session:
            yield session

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.budget.get_postgres_session", live_budget_session
    )
    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )

    shadow_provider = _FakeProvider()
    shadow_guarded = attach_qua_shadow_budget_guard(
        shadow_provider,
        org_id=org_id,
        provider=_PROVIDER,
        model=_MODEL,
        base_url=None,
    )
    with pytest.raises(AgentProviderError) as shadow_caught:
        await shadow_guarded.decide(
            [AgentMessage(AgentMessageRole.USER, "q1")], (), {"type": "object"}, 1, 64
        )
    assert shadow_caught.value.code is AgentProviderErrorCode.BUDGET_EXHAUSTED
    assert shadow_provider.calls == 0

    live_provider = _FakeProvider()
    async with session_maker() as unused_session:
        live_guarded = attach_agent_budget_guard(
            live_provider,
            session=unused_session,
            org_id=org_id,
            provider=_PROVIDER,
            model=_MODEL,
            base_url=None,
        )
    live_result = await live_guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "q2")], (), {"type": "object"}, 1, 64
    )
    assert live_result.decision.value == {"ok": True}
    assert live_provider.calls == 1


@pytest.mark.asyncio
async def test_unset_env_defaults_to_the_conservative_ceiling_not_unlimited():
    assert (
        qua_shadow_budget.qua_shadow_maximum_micro_usd()
        == DEFAULT_QUA_SHADOW_MAX_MICRO_USD
    )
    assert DEFAULT_QUA_SHADOW_MAX_MICRO_USD > 0


@pytest.mark.asyncio
async def test_mutation_pointing_the_shadow_guard_at_the_live_table_breaks_isolation(
    session_maker, monkeypatch
):
    """Mutation-style proof the isolation tests above actually discriminate:
    if ``qua_shadow_budget``'s reservation model is mutated to the SAME
    class the live guard uses, a shadow call now writes into
    ``byo_llm_budget_reservations`` -- exactly the contamination the real
    (unmutated) code must never produce. Confirms the two isolation tests
    above would catch a future accidental pool-merge.
    """

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )
    # THE MUTATION: point the shadow module's reservation model at the
    # live table's model class.
    monkeypatch.setattr(
        qua_shadow_budget, "QUAShadowBudgetReservation", BYOLLMBudgetReservation
    )

    provider = _FakeProvider()
    guarded = attach_qua_shadow_budget_guard(
        provider, org_id=org_id, provider=_PROVIDER, model=_MODEL, base_url=None
    )
    await guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "question")], (), {"type": "object"}, 1, 64
    )

    async with session_maker() as session:
        live_rows = list(
            (await session.execute(select(BYOLLMBudgetReservation))).scalars()
        )
    # Under the real (unmutated) module this list is always empty -- see
    # test_shadow_call_never_writes_the_live_reservation_table above. Under
    # this deliberate mutation it is not, proving that test's assertion is
    # load-bearing rather than vacuous.
    assert len(live_rows) == 1
    assert live_rows[0].provider == _PROVIDER


# ---------------------------------------------------------------------------
# Codex round 1 findings: usage accounting and reservation sizing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_dispatched_failure_with_attached_usage_bills_its_real_cost(
    session_maker, monkeypatch
):
    """Codex round 1 (HIGH, confirmed): a dispatched-but-failed call can
    carry REAL billable usage (e.g. AgentProviderError.usage on
    OUTPUT_EXHAUSTED) -- discarding it reconciled every such call as
    ``usage_unavailable`` regardless of whether real usage was available.
    The reservation must reconcile to its ACTUAL cost, not an unreported
    one, whenever the provider attached it."""

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )

    error = AgentProviderError(
        AgentProviderErrorCode.OUTPUT_EXHAUSTED,
        usage=AgentUsage(input_tokens=40, output_tokens=256, cached_input_tokens=0),
    )
    provider = _RaisingProvider(error)
    guarded = attach_qua_shadow_budget_guard(
        provider, org_id=org_id, provider=_PROVIDER, model=_MODEL, base_url=None
    )
    with pytest.raises(AgentProviderError) as caught:
        await guarded.decide(
            [AgentMessage(AgentMessageRole.USER, "q")], (), {"type": "object"}, 1, 64
        )
    assert caught.value.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED

    expected_cost = cost_micro_usd(
        provider=_PROVIDER,
        model=_MODEL,
        base_url=None,
        input_tokens=40,
        output_tokens=256,
        cached_input_tokens=0,
    )
    assert expected_cost is not None

    async with session_maker() as session:
        rows = list(
            (await session.execute(select(QUAShadowBudgetReservation))).scalars()
        )
    assert len(rows) == 1
    assert rows[0].status == "failed"
    assert rows[0].actual_micro_usd == expected_cost


@pytest.mark.asyncio
async def test_a_dispatched_failure_with_no_usage_still_counts_its_reserved_amount(
    session_maker, monkeypatch
):
    """Codex round 1 (HIGH, confirmed): the original `used` filter excluded
    BOTH 'voided' and 'usage_unavailable' reservations, so a dispatched
    call that failed with no usage attached advanced ZERO quota -- a
    provider that always fails this way could be called forever without
    ever exhausting the isolated cap, defeating it entirely. A
    'usage_unavailable' reservation must still count at its RESERVED
    (worst-case) amount, exactly like llm.budget.get_budget_status's own
    filter for the live pool -- proven here by configuring the ceiling to
    exactly one such reservation's size and showing a second call is
    rejected."""

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )

    messages = [AgentMessage(AgentMessageRole.USER, "q1")]
    tools: tuple = ()
    response_schema = {"type": "object"}
    payload = json.dumps(
        {
            "messages": [str(item) for item in messages],
            "tools": [str(item) for item in tools],
            "response_schema": response_schema,
        },
        sort_keys=True,
        default=str,
    )
    maximum_input_tokens = max(1, len(payload.encode("utf-8")))
    reserved = cost_micro_usd(
        provider=_PROVIDER,
        model=_MODEL,
        base_url=None,
        input_tokens=maximum_input_tokens,
        output_tokens=64,
        cached_input_tokens=0,
    )
    assert reserved is not None
    # Exactly enough for the first call's reservation -- nothing left over.
    monkeypatch.setenv(QUA_SHADOW_BUDGET_ENV_KEY, str(reserved))

    error = AgentProviderError(AgentProviderErrorCode.PROVIDER_UNAVAILABLE)
    provider = _RaisingProvider(error)
    guarded = attach_qua_shadow_budget_guard(
        provider, org_id=org_id, provider=_PROVIDER, model=_MODEL, base_url=None
    )
    with pytest.raises(AgentProviderError) as first:
        await guarded.decide(messages, tools, response_schema, 1, 64)
    assert first.value.code is AgentProviderErrorCode.PROVIDER_UNAVAILABLE

    async with session_maker() as session:
        rows = list(
            (await session.execute(select(QUAShadowBudgetReservation))).scalars()
        )
    assert len(rows) == 1
    assert rows[0].status == "usage_unavailable"
    assert rows[0].actual_micro_usd is None

    # A SECOND shadow call is rejected: the first failure's RESERVED
    # amount already counts toward the (now fully consumed) quota -- the
    # cap cannot be evaded by repeated dispatched-but-unreconciled failures.
    with pytest.raises(AgentProviderError) as second:
        await guarded.decide(messages, tools, response_schema, 1, 64)
    assert second.value.code is AgentProviderErrorCode.BUDGET_EXHAUSTED
    assert provider.calls == 1  # the second call never dispatched


@pytest.mark.asyncio
async def test_reservation_accounts_for_the_full_wire_payload_not_just_messages(
    session_maker, monkeypatch
):
    """Codex round 1 (HIGH, confirmed): the reservation must bound the
    COMPLETE wire request -- messages AND tools AND response_schema -- not
    just the messages. A messages-only estimate is not a guaranteed upper
    bound on the real request (this seam's own JSON Schema can be sizeable
    once the shortlist is large), so it could admit a call a full-payload
    estimate would correctly reject. Proven by picking a ceiling strictly
    between the two estimates for the SAME call and showing the real guard
    rejects it (a messages-only guard would have wrongly admitted it)."""

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )

    tiny_messages = [AgentMessage(AgentMessageRole.USER, "q")]
    large_schema = {
        "type": "object",
        "properties": {f"field_{i}": {"type": "string"} for i in range(500)},
    }

    messages_only_estimate = max(1, sum(len(str(item)) for item in tiny_messages))
    full_payload = json.dumps(
        {
            "messages": [str(item) for item in tiny_messages],
            "tools": [],
            "response_schema": large_schema,
        },
        sort_keys=True,
        default=str,
    )
    full_payload_estimate = max(1, len(full_payload.encode("utf-8")))
    assert full_payload_estimate > messages_only_estimate * 10  # sanity

    messages_only_cost = cost_micro_usd(
        provider=_PROVIDER,
        model=_MODEL,
        base_url=None,
        input_tokens=messages_only_estimate,
        output_tokens=64,
        cached_input_tokens=0,
    )
    full_payload_cost = cost_micro_usd(
        provider=_PROVIDER,
        model=_MODEL,
        base_url=None,
        input_tokens=full_payload_estimate,
        output_tokens=64,
        cached_input_tokens=0,
    )
    assert messages_only_cost is not None
    assert full_payload_cost is not None
    assert full_payload_cost > messages_only_cost

    ceiling = (messages_only_cost + full_payload_cost) // 2
    assert messages_only_cost <= ceiling < full_payload_cost
    monkeypatch.setenv(QUA_SHADOW_BUDGET_ENV_KEY, str(ceiling))

    provider = _FakeProvider()
    guarded = attach_qua_shadow_budget_guard(
        provider, org_id=org_id, provider=_PROVIDER, model=_MODEL, base_url=None
    )
    with pytest.raises(AgentProviderError) as caught:
        await guarded.decide(tiny_messages, (), large_schema, 1, 64)
    assert caught.value.code is AgentProviderErrorCode.BUDGET_EXHAUSTED
    assert provider.calls == 0  # rejected before dispatch -- never admitted


@pytest.mark.asyncio
async def test_a_successful_call_with_unreported_cache_usage_is_not_discarded(
    session_maker, monkeypatch
):
    """Codex round 2 (MEDIUM, confirmed): the shadow guard used to RAISE
    (discarding an otherwise-successful, evaluated decision) whenever
    reconciliation could not confirm a cost -- e.g. the provider's response
    omitted cache-usage detail, which OpenAICompatibleAgentProvider's own
    ``_normalize_usage`` explicitly leaves as ``None`` rather than
    defaulting to 0 when absent. Unlike the live BYO guard (protecting the
    org's own configured monetary ceiling), the shadow quota already counts
    this reservation's RESERVED amount toward `used` regardless (round 1's
    fix) -- so the call's real, successful decision must be returned to the
    caller, not thrown away."""

    org_id = str(uuid.uuid4())
    await _seed_org(session_maker, org_id)

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )

    class _NoCacheDetailProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def decide(
            self,
            messages,
            tools,
            response_schema,
            timeout_seconds,
            max_output_tokens,
            signal=None,
        ) -> AgentDecisionResult:
            self.calls += 1
            return AgentDecisionResult(
                decision=AgentFinalAnswer(value={"real": "decision"}),
                # cached_input_tokens left at its dataclass default (None)
                # -- exactly what _normalize_usage produces when the
                # provider's response omits prompt_tokens_details.
                usage=AgentUsage(input_tokens=5, output_tokens=5),
                latency_ms=1,
                provider_fingerprint="fake",
                model_fingerprint="fake",
            )

        async def aclose(self) -> None:
            return None

    provider = _NoCacheDetailProvider()
    guarded = attach_qua_shadow_budget_guard(
        provider, org_id=org_id, provider=_PROVIDER, model=_MODEL, base_url=None
    )
    # Must NOT raise -- the real decision is returned.
    result = await guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "q")], (), {"type": "object"}, 1, 64
    )
    assert result.decision.value == {"real": "decision"}
    assert provider.calls == 1

    async with session_maker() as session:
        rows = list(
            (await session.execute(select(QUAShadowBudgetReservation))).scalars()
        )
    assert len(rows) == 1
    assert rows[0].status == "usage_unavailable"
    assert rows[0].actual_micro_usd is None
