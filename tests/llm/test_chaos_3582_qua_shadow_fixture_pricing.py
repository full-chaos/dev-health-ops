"""CHAOS-3582: the isolated QUA shadow budget guard failed closed on the Ask
Dev acceptance stack's own scripted provider, unconditionally.

Found by the CHAOS-3532 live verify (2026-08-07): with the QUA shadow armed,
``dev_run_qua_shadow`` never reached ``evaluated`` -- every call reported
``status=skipped_budget_exhausted``, ``error_class=budget_unavailable``, with
ZERO rows in ``dev_qua_shadow_budget_reservations`` (it failed before ever
reserving, not from quota depletion).

Root cause, traced to ``llm.budget.reliable_price``: it requires
``_official_openai_endpoint(base_url)`` -- true only for an empty base_url or
exactly ``https://api.openai.com`` -- before it will even consult the price
table. The acceptance stack's scripted provider is served from
``http://ask-dev-scripted-openai:8001/v1`` by construction
(``tests/acceptance/compose.ask-dev.yml``), so it was NEVER priced, and
``guard_qua_shadow_call`` (``llm/qua_shadow_budget.py``) treats an unpriced
provider/model as an accounting error and fails closed -- unlike the live/BYO
guard, which lets an unpriced custom provider through unbudgeted. That
asymmetry is deliberate (a real customer's unpriced spend must never be
silently reported as free) and this fix does not touch it: the scripted
provider is not unbudgeted here, it is honestly, explicitly priced -- the
SAME carve-out ``llm.agent.openai_compatible`` already made for the SAME
model id on the live platform-cost path (CHAOS-3552).

RED-first: every assertion in ``test_the_acceptance_fixture_is_priced_on_its_
own_non_official_endpoint`` and
``test_the_qua_shadow_guard_actually_reaches_evaluated_for_the_fixture``
fails against ``reliable_price``/``guard_qua_shadow_call`` before this
ticket's fix (``_PRICE_PER_MILLION`` had no ``ask-dev-scripted-v1`` entry, and
``reliable_price`` checked the endpoint before any carve-out). The
anti-widening tests below must stay green in both states -- they pin that the
fix does not fail OPEN for anything else.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.llm.agent.contracts import (
    AgentDecisionResult,
    AgentFinalAnswer,
    AgentMessage,
    AgentMessageRole,
    AgentUsage,
)
from dev_health_ops.llm.agent.scripted_openai_service import SCRIPTED_OPENAI_MODEL
from dev_health_ops.llm.budget import reliable_price
from dev_health_ops.llm.qua_shadow_budget import attach_qua_shadow_budget_guard
from dev_health_ops.models.git import Base
from dev_health_ops.models.llm_budget import QUAShadowBudgetReservation
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

#: The acceptance stack's real, internal-only base_url
#: (``tests/acceptance/compose.ask-dev.yml``'s
#: ``ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL``) -- never ``api.openai.com``, and
#: that gap is the entire defect.
_ACCEPTANCE_BASE_URL = "http://ask-dev-scripted-openai:8001/v1"


def test_the_acceptance_fixture_is_priced_on_its_own_non_official_endpoint() -> None:
    """The exact call shape ``guard_qua_shadow_call`` makes for the
    acceptance stack must resolve to a real price, not ``None``."""

    price = reliable_price(
        provider="openai", model=SCRIPTED_OPENAI_MODEL, base_url=_ACCEPTANCE_BASE_URL
    )
    assert price is not None, (
        "the acceptance fixture must be priced on its own scripted endpoint "
        "-- an unpriced result here is exactly CHAOS-3582's defect"
    )
    input_rate, cached_input_rate, output_rate = price
    assert input_rate > 0
    assert cached_input_rate > 0
    assert output_rate > 0


@pytest.mark.parametrize(
    "model",
    [
        f"{SCRIPTED_OPENAI_MODEL}-v2",
        f"{SCRIPTED_OPENAI_MODEL}-",
        f"not-{SCRIPTED_OPENAI_MODEL}",
        SCRIPTED_OPENAI_MODEL.rstrip("1") + "2",
    ],
)
def test_fixture_carve_out_does_not_widen_to_neighbouring_model_ids(
    model: str,
) -> None:
    """Anti-widening control, mirroring CHAOS-3552's own: the carve-out is
    an EXACT model-id match. A stem-sharing neighbour on the same
    non-official endpoint must stay unpriced -- exactly the fail-closed
    posture CHAOS-3582 is not allowed to weaken."""

    assert (
        reliable_price(provider="openai", model=model, base_url=_ACCEPTANCE_BASE_URL)
        is None
    )


def test_a_real_custom_endpoint_model_stays_unpriced() -> None:
    """The general fail-closed rule for a genuine BYO/custom endpoint is
    untouched: a real customer's self-hosted gateway serving a real model
    must still be unpriced, so the live guard's own accounting stays honest
    about what it cannot know."""

    assert (
        reliable_price(
            provider="openai",
            model="gpt-4o",
            base_url="https://my-byo-gateway.example/v1",
        )
        is None
    )


def test_a_real_openai_model_on_an_unlisted_endpoint_stays_unpriced() -> None:
    """Same control, non-acceptance flavour: a real OpenAI-compatible
    gateway (Azure, OpenRouter, a corporate proxy) serving a REAL model must
    not be swept into the fixture carve-out just because it shares the
    ``openai`` provider name."""

    assert (
        reliable_price(
            provider="openai",
            model="gpt-5-mini",
            base_url="https://my-corporate-openai-gateway.example/v1",
        )
        is None
    )


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'qua_shadow.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(Organization, QUAShadowBudgetReservation),
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


class _FakeScriptedProvider:
    """Mirrors ``scripted_openai_service``'s QUA answer shape closely enough
    for the guard's usage extraction and reconciliation -- the guard itself,
    not the HTTP transport, is what this test exercises."""

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
            decision=AgentFinalAnswer(
                value={"schema_version": "dev_question_understanding.v1"}
            ),
            usage=AgentUsage(input_tokens=42, output_tokens=17, cached_input_tokens=0),
            latency_ms=1,
            provider_fingerprint="fake-scripted",
            model_fingerprint="fake-scripted",
        )

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_the_qua_shadow_guard_actually_reaches_evaluated_for_the_fixture(
    session_maker, monkeypatch
) -> None:
    """End-to-end reproduction of CHAOS-3582's exact reported symptom:
    ``guard_qua_shadow_call`` against the acceptance provider/model/base_url
    triple must reserve, invoke, and reconcile as ``succeeded`` -- not raise
    ``QUAShadowBudgetAccountingError`` before ever calling the provider.

    Before the fix: this raises ``QUAShadowBudgetAccountingError`` and
    ``provider.calls`` stays ``0`` -- the scripted service is never even
    invoked, matching the live boot's zero rows in
    ``dev_qua_shadow_budget_reservations``.
    """

    org_id = str(uuid.uuid4())
    async with session_maker() as session:
        session.add(
            Organization(
                id=uuid.UUID(org_id),
                slug=f"qua-shadow-pricing-{org_id[:8]}",
                name="QUA Shadow Pricing Test Organization",
                tier="team",
            )
        )
        await session.commit()

    @asynccontextmanager
    async def shadow_budget_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(
        "dev_health_ops.llm.qua_shadow_budget.get_postgres_session",
        shadow_budget_session,
    )

    provider = _FakeScriptedProvider()
    guarded = attach_qua_shadow_budget_guard(
        provider,
        org_id=org_id,
        provider="openai",
        model=SCRIPTED_OPENAI_MODEL,
        base_url=_ACCEPTANCE_BASE_URL,
    )
    result = await guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "question")], (), {"type": "object"}, 1, 64
    )
    assert result.decision.value == {"schema_version": "dev_question_understanding.v1"}
    assert provider.calls == 1, "the scripted provider must actually be invoked"

    async with session_maker() as session:
        rows = list(
            (await session.execute(select(QUAShadowBudgetReservation))).scalars()
        )
    assert len(rows) == 1, (
        "exactly one reservation must exist -- CHAOS-3582 observed ZERO "
        "because the guard raised before ever reserving"
    )
    assert rows[0].status == "succeeded"
    assert rows[0].actual_micro_usd is not None
    assert rows[0].actual_micro_usd >= 0
