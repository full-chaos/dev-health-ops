"""CHAOS-3582: the isolated QUA shadow budget guard failed closed on
unpriced provider/model pairs, in TWO distinct ways.

Shape 1 -- found by the CHAOS-3532 live verify (2026-08-07) against the Ask
Dev acceptance stack: with the QUA shadow armed, ``dev_run_qua_shadow`` never
reached ``evaluated`` -- every call reported
``status=skipped_budget_exhausted``, ``error_class=budget_unavailable``, with
ZERO rows in ``dev_qua_shadow_budget_reservations`` (it failed before ever
reserving, not from quota depletion). Root cause: ``llm.budget.reliable_price``
requires ``_official_openai_endpoint(base_url)`` -- true only for an empty
base_url or exactly ``https://api.openai.com`` -- before it will even
consult the price table, and the acceptance stack's scripted provider is
served from ``http://ask-dev-scripted-openai:8001/v1`` by construction, so
it was never priced.

Shape 2 -- found immediately after, live-reproduced in the SHARED DEV STACK
with the REAL provider: the same failure, for a DIFFERENT reason. Dev has no
base_url override at all (official endpoint), but ``ops/.env`` configures
``LLM_MODEL="gpt-5-nano"``, and ``_PRICE_PER_MILLION`` had only ever priced
``gpt-5-mini``. Every ``dev_run_qua_shadow`` row since the flags were armed
reported the identical ``skipped_budget_exhausted`` / ``budget_unavailable``
symptom -- CHAOS-3389 has no shadow evidence from dev, and CHAOS-3525's
commit mode reads as inert to a user despite being armed.

Fix for both: honest pricing, never a fail-open exemption.
* Shape 1 -- an exact-match carve-out mirroring the SAME one
  ``llm.agent.openai_compatible`` already made for the SAME model id on the
  live platform-cost path (CHAOS-3552), checked before the endpoint test.
* Shape 2 -- ``reliable_price`` borrows a REAL model's rate from
  ``openai_compatible._PLATFORM_MODEL_PRICES`` (already sourced and cited)
  when its own table lacks an entry, but ONLY after the official-endpoint
  check passes -- a real model on a genuine non-official/BYO endpoint still
  resolves to no price, unchanged.

RED-first: every assertion in the two ``..._is_priced_on...`` tests and the
two ``..._guard_actually_reaches_evaluated...`` end-to-end tests fails
against ``reliable_price``/``guard_qua_shadow_call`` before this ticket's
fix. The anti-widening tests must stay green in both states -- they pin that
the fix does not fail OPEN for anything else: a stem-sharing neighbour
model id, a real model on a genuine custom/BYO endpoint, and a genuinely
unpriced real model on the OFFICIAL endpoint (neither table has ever heard
of it) must all still resolve to ``None``.

Review finding (BLOCKING, confidence 90, fixed here): an earlier revision of
the shape-1 carve-out matched on MODEL NAME alone, with no transport check.
Confirmed live before the fix: a BYO tenant naming THEIR OWN model, on THEIR
OWN real endpoint, literally ``"ask-dev-scripted-v1"`` got priced at the
fixture's near-zero rate -- on the real OpenAI endpoint, with no base_url
override at all, AND (a second leak the same review turned up) even via the
generic ``_PRICE_PER_MILLION`` official-endpoint lookup once the fixture had
an entry there. Fixed by requiring BOTH the model id AND the acceptance
stack's own scripted transport (``_is_acceptance_scripted_transport``), and
by keeping the fixture's price OUT of ``_PRICE_PER_MILLION`` entirely so
there is no path back into the shared, name-only lookup.
``test_a_byo_tenant_cannot_name_their_way_into_the_fixture_price`` and
``test_the_fixture_transport_gate_requires_an_exact_match`` are the
permanent regression coverage for this.
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
    ("case_id", "base_url"),
    [
        ("official_openai_endpoint", "https://api.openai.com"),
        ("tenants_own_byo_gateway", "https://tenant-byo-gateway.example.com/v1"),
        ("no_base_url_override_at_all", None),
    ],
)
def test_a_byo_tenant_cannot_name_their_way_into_the_fixture_price(
    case_id: str, base_url: str | None
) -> None:
    """Review finding (BLOCKING, confidence 90): model name alone is
    tenant-controlled for a BYO configuration. Before the transport gate, a
    tenant naming THEIR OWN model literally ``"ask-dev-scripted-v1"`` got
    priced at the fixture's near-zero rate on every one of these three real
    call shapes -- verified live, including through the generic
    ``_PRICE_PER_MILLION`` official-endpoint lookup once the fixture had an
    entry there. Not the acceptance stack's own transport in any of these
    three cases, so all three must resolve to ``None``."""

    assert (
        reliable_price(
            provider="openai", model=SCRIPTED_OPENAI_MODEL, base_url=base_url
        )
        is None
    ), f"{case_id}: a non-acceptance transport must never get the fixture price"


@pytest.mark.parametrize(
    ("case_id", "base_url"),
    [
        # Right host and port, wrong (TLS) scheme.
        (
            "https_scheme_on_the_right_host_and_port",
            "https://ask-dev-scripted-openai:8001/v1",
        ),
        # Right host and scheme, wrong port.
        ("wrong_port_on_the_right_host", "http://ask-dev-scripted-openai:9999/v1"),
        # Right scheme and port, wrong host.
        ("wrong_host_on_the_right_port", "http://not-the-scripted-service:8001/v1"),
    ],
)
def test_the_fixture_transport_gate_requires_an_exact_match(
    case_id: str, base_url: str
) -> None:
    """The transport gate is an exact (scheme, host, port) match, not a
    substring or partial one -- a near-miss must still fail closed, the
    same discipline the model-id anti-widening tests already apply to the
    OTHER half of the carve-out's condition."""

    assert (
        reliable_price(
            provider="openai", model=SCRIPTED_OPENAI_MODEL, base_url=base_url
        )
        is None
    ), f"{case_id}: a near-miss transport must never get the fixture price"


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


#: ``ops/.env``'s actual configured model -- the shared dev stack's own
#: reported failure shape, no base_url override at all.
_DEV_STACK_MODEL = "gpt-5-nano"


def test_the_dev_stack_configured_model_is_priced_on_the_official_endpoint() -> None:
    """Shape 2: a real model with no price of its own in this table, on the
    OFFICIAL endpoint (dev's actual configuration -- no base_url override),
    must resolve to a real, non-fabricated, cited price rather than staying
    unpriced forever until someone hand-adds a literal for it."""

    price = reliable_price(provider="openai", model=_DEV_STACK_MODEL, base_url=None)
    assert price is not None, (
        "the dev stack's own configured model must be priced on the "
        "official endpoint -- an unpriced result here is CHAOS-3582 shape 2"
    )
    input_rate, cached_input_rate, output_rate = price
    assert input_rate > 0
    assert cached_input_rate > 0
    assert output_rate > 0
    # Borrowed from openai_compatible._PLATFORM_MODEL_PRICES verbatim --
    # never re-derived or approximated for the two legs that ARE sourced.
    assert (input_rate, output_rate) == (50_000, 400_000)


def test_a_dated_snapshot_of_the_dev_stack_model_resolves_the_same_price() -> None:
    """Parity with the live path's own tested behaviour
    (``test_chaos_3552_platform_price_book.py``): a dated model snapshot
    (``gpt-5-nano-2026-01-01``) resolves to the SAME borrowed rate as the
    bare id, via the same canonicalization the live path already uses."""

    assert reliable_price(
        provider="openai", model=f"{_DEV_STACK_MODEL}-2026-01-01", base_url=None
    ) == reliable_price(provider="openai", model=_DEV_STACK_MODEL, base_url=None)


def test_the_dev_stack_model_on_a_custom_endpoint_still_stays_unpriced() -> None:
    """Borrowing a real model's rate must still respect the endpoint check
    -- unlike the fixture, a REAL model's cost on a genuine non-official
    endpoint is unknown, not zero and not OpenAI's rate."""

    assert (
        reliable_price(
            provider="openai",
            model=_DEV_STACK_MODEL,
            base_url="https://my-byo-gateway.example/v1",
        )
        is None
    )


def test_a_genuinely_unpriced_model_on_the_official_endpoint_still_fails_closed() -> (
    None
):
    """Borrowing only ever WIDENS what gets priced with REAL, sourced rates
    -- it must never fabricate a price for a model neither table has ever
    heard of. ``None`` here is the correct, unchanged, fail-closed answer;
    an operator misconfiguration must still be loud, not silently free."""

    assert (
        reliable_price(
            provider="openai", model="a-model-nobody-has-priced", base_url=None
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
@pytest.mark.parametrize(
    ("case_id", "model", "base_url"),
    [
        (
            "shape1_acceptance_fixture_on_its_scripted_endpoint",
            SCRIPTED_OPENAI_MODEL,
            _ACCEPTANCE_BASE_URL,
        ),
        (
            "shape2_dev_stack_model_on_the_official_endpoint",
            _DEV_STACK_MODEL,
            None,
        ),
    ],
)
async def test_the_qua_shadow_guard_actually_reaches_evaluated(
    session_maker, monkeypatch, case_id: str, model: str, base_url: str | None
) -> None:
    """End-to-end reproduction of CHAOS-3582's exact reported symptom, both
    shapes: ``guard_qua_shadow_call`` against the real provider/model/
    base_url triple must reserve, invoke, and reconcile as ``succeeded`` --
    not raise ``QUAShadowBudgetAccountingError`` before ever calling the
    provider.

    Before the corresponding fix: this raises ``QUAShadowBudgetAccountingError``
    and ``provider.calls`` stays ``0`` -- the provider is never even invoked,
    matching the live boot's / dev stack's own zero/absent reservation rows.
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
        model=model,
        base_url=base_url,
    )
    result = await guarded.decide(
        [AgentMessage(AgentMessageRole.USER, "question")], (), {"type": "object"}, 1, 64
    )
    assert result.decision.value == {"schema_version": "dev_question_understanding.v1"}
    assert provider.calls == 1, f"{case_id}: the provider must actually be invoked"

    async with session_maker() as session:
        rows = list(
            (await session.execute(select(QUAShadowBudgetReservation))).scalars()
        )
    assert len(rows) == 1, (
        f"{case_id}: exactly one reservation must exist -- CHAOS-3582 "
        "observed ZERO because the guard raised before ever reserving"
    )
    assert rows[0].status == "succeeded"
    assert rows[0].actual_micro_usd is not None
    assert rows[0].actual_micro_usd >= 0
