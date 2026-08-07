"""CHAOS-3552: an unmeterable platform model must never be booked SILENTLY.

``ProviderBudget.require`` reserves US$1 per model call. ``ProviderBudget.add``
reconciles that reservation down to the real cost -- but only when a real cost
exists. ``_estimated_cost_microusd`` returns ``None`` for any model outside
``_PLATFORM_MODEL_PRICES``, and the reconciliation branch leaves the reservation
standing:

    if usage.estimated_cost_microusd is None:
        reconciled_cost = prior_cost      # reservation stands, unreconciled

The dev stack sets ``LLM_MODEL="gpt-5-nano"``, which is not in the price book.
Measured consequence, with the request cap at 1,000/month:

    booked per run        US$4.00   (4 rounds x the US$1 reservation)
    real per run          US$0.018  (were it priced)
    overcharge            222x
    runs before the US$100 cost cap    25   -- 40x tighter than the request cap

That is what CHAOS-3523's "the allowance is gating platform runs" actually was.

**The invariant this module enforces is NOT the one the ticket proposed.**
CHAOS-3552 suggested "assert every certified model is priced", which is not
implementable: ``CERTIFIED_PLATFORM_AGENT_PROVIDERS`` lists PROVIDERS
(``openai``, ``local``, ``ollama``, ``lmstudio``), not models, and the model is
operator-configured at runtime. Self-hosted providers are *deliberately*
unpriced -- ``budget.py`` states it outright: "Only exact, server-certified
provider/model pairs are present. An absent pair is unavailable, never zero."
Requiring a price for every certified provider would break every self-hosted
deployment.

The correct invariant, which replaces it:

    A platform provider whose cost cannot be priced must never silently book
    the reservation as its cost.

It splits by WHY the price is missing, and the two halves want opposite
treatment:

* ``openai`` on an official endpoint with an unpriced model -- an operator
  misconfiguration. Books the conservative reservation, **loudly** (warning
  with model + remedy, and a metric). It does NOT refuse construction: with a
  three-entry price book, refusing would have removed Ask Dev from every
  organization running gpt-4o, gpt-5 or o3, which is far larger harm than an
  overstated allowance.
* ``openai`` on a NON-official endpoint (Azure, OpenRouter, a corporate
  gateway) -- billability **unknown**, so it also books the reservation
  loudly, and never reports zero. Reporting these as free was a fail-OPEN
  defect an earlier revision shipped; see
  ``test_a_billable_gateway_is_never_reported_as_free``.
* self-hosted BY PROVIDER (``local``/``ollama``/``lmstudio``) -- the
  operator's own hardware, genuinely free. Cost is an explicit **0**, so the
  reservation reconciles away and no charge is booked.
* ``ask-dev-scripted-v1`` -- a deterministic test double, not a production
  model. Carved out explicitly, keyed to the exact id, and guarded below
  against widening.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dev_health_ops.llm.agent.openai_compatible import (
    SCRIPTED_FIXTURE_MODEL,
    PlatformCostMetering,
    platform_cost_metering,
)

#: The model the dev stack actually configures (`ops/.env`: LLM_MODEL).
DEV_STACK_MODEL = "gpt-5-nano"

#: An endpoint that is NOT api.openai.com. Pricing an OpenAI model name served
#: from here at OpenAI rates would be a fabricated cost.
SELF_HOSTED_URL = "https://llm.internal.example/v1"


# --------------------------------------------------------------------------
# The gap this ticket exists to close.
# --------------------------------------------------------------------------


def test_the_configured_dev_stack_model_is_priced() -> None:
    """RED on today's gap: `gpt-5-nano` has no price entry.

    This is the whole defect in one assertion. Observed failing before the
    price was added -- the ordering matters, because a price added first and
    a test written after proves only that the dict contains what the dict
    contains.
    """

    assert (
        platform_cost_metering(provider="openai", model=DEV_STACK_MODEL, base_url=None)
        is PlatformCostMetering.PRICED
    )


def test_a_dated_variant_of_a_priced_model_is_still_priced() -> None:
    """Providers pin dated snapshots; the price book must follow them.

    `gpt-5-nano-2026-01-01` is the same model at the same rates. Without this
    a routine provider-side pin silently reopens the whole defect.
    """

    assert (
        platform_cost_metering(
            provider="openai", model=f"{DEV_STACK_MODEL}-2026-01-01", base_url=None
        )
        is PlatformCostMetering.PRICED
    )


# --------------------------------------------------------------------------
# The loud failure, and the two things it must NOT do.
# --------------------------------------------------------------------------


def test_an_unpriced_openai_model_on_the_official_endpoint_is_a_config_error() -> None:
    """The fail-loud half of the invariant.

    An operator who points `LLM_MODEL` at a real OpenAI model this build has
    no price for gets a configuration error, not a run that reports costs
    wrong by two orders of magnitude.
    """

    assert (
        platform_cost_metering(
            provider="openai", model="gpt-6-imaginary", base_url=None
        )
        is PlatformCostMetering.UNPRICED_CONFIGURATION_ERROR
    )


@pytest.mark.parametrize(
    ("provider", "base_url"),
    [
        ("local", SELF_HOSTED_URL),
        ("ollama", SELF_HOSTED_URL),
        ("lmstudio", SELF_HOSTED_URL),
    ],
)
def test_self_hosted_is_unmetered_rather_than_a_config_error(
    provider: str, base_url: str
) -> None:
    """The no-meter half. Self-hosted must not fail construction.

    These deployments have no dollar cost to report and pricing them at
    OpenAI's rates would fabricate one. They are unmetered by design --
    distinct from "we forgot to price it", which is the case above.
    """

    assert (
        platform_cost_metering(
            provider=provider, model="llama-3.1-70b", base_url=base_url
        )
        is PlatformCostMetering.UNMETERED_SELF_HOSTED
    )


def test_a_self_hosted_endpoint_claiming_an_openai_model_name_is_not_priced() -> None:
    """Endpoint blindness, asserted at the classifier.

    The agent price book keys on model name alone, so a self-hosted endpoint
    serving something it calls `gpt-5-mini` is billed at OpenAI's rates today
    while `budget.reliable_price` correctly declines. Stage 2 removes the
    duplicate book; this pins the classifier's answer meanwhile.
    """

    assert (
        platform_cost_metering(
            provider="openai", model="gpt-5-mini", base_url=SELF_HOSTED_URL
        )
        is PlatformCostMetering.UNKNOWN_BILLABILITY
    )


# --------------------------------------------------------------------------
# The carve-out, and the control that stops it widening.
# --------------------------------------------------------------------------


def test_the_scripted_fixture_model_is_carved_out() -> None:
    """`ask-dev-scripted-v1` is a test double, not an unpriced production model.

    It must not fail construction: the acceptance stack runs on it, and a
    naive loud failure would take that stack down.
    """

    assert (
        platform_cost_metering(
            provider="openai", model=SCRIPTED_FIXTURE_MODEL, base_url=None
        )
        is PlatformCostMetering.FIXTURE
    )


def test_the_carve_out_cannot_widen_by_prefix() -> None:
    """THE anti-widening control (team-lead condition).

    The carve-out is keyed to the exact model id. Every other unpriced
    openai-official model must still fail loud WITH the carve-out present --
    otherwise the escape hatch quietly becomes the rule, which is how a
    deliberate exception turns into an accidental default.

    Prefix and suffix neighbours are included because the surrounding price
    lookup DOES match on prefix (`model.startswith(f"{known}-")`), so a
    carve-out written in the same style would silently admit all of these.
    """

    for impostor in (
        f"{SCRIPTED_FIXTURE_MODEL}-v2",
        f"{SCRIPTED_FIXTURE_MODEL}-",
        f"not-{SCRIPTED_FIXTURE_MODEL}",
        SCRIPTED_FIXTURE_MODEL.rstrip("1") + "2",
        SCRIPTED_FIXTURE_MODEL.upper(),
    ):
        assert (
            platform_cost_metering(provider="openai", model=impostor, base_url=None)
            is PlatformCostMetering.UNPRICED_CONFIGURATION_ERROR
        ), f"carve-out widened to admit {impostor!r}"


def test_the_carve_out_is_exactly_one_model() -> None:
    """Stated as a count, so adding a second fixture is a deliberate edit.

    A carve-out that can grow without anyone noticing is not an exception, it
    is a second price book.
    """

    from dev_health_ops.llm.agent.openai_compatible import _CARVE_OUT_MODELS

    assert _CARVE_OUT_MODELS == frozenset({SCRIPTED_FIXTURE_MODEL})


# --------------------------------------------------------------------------
# Claims this change makes in prose, asserted so they cannot rot.
# --------------------------------------------------------------------------


def test_the_carve_out_id_matches_its_actual_producer() -> None:
    """`SCRIPTED_FIXTURE_MODEL` must equal the scripted service's own constant.

    The carve-out comment says it names
    ``scripted_openai_service.SCRIPTED_OPENAI_MODEL`` "rather than a bare
    literal". It IS a bare literal today (importing the scripted service into
    the provider module for one string is not worth the import edge), so this
    is what makes the claim true: if the producer ever renames its model id,
    the carve-out stops matching and the acceptance stack starts failing loud
    -- and this test says so first, at the rename, instead of at 3am.
    """

    from dev_health_ops.llm.agent.scripted_openai_service import SCRIPTED_OPENAI_MODEL

    assert SCRIPTED_FIXTURE_MODEL == SCRIPTED_OPENAI_MODEL


@pytest.mark.parametrize(
    "base_url",
    [
        None,
        "",
        "   ",
        "https://api.openai.com",
        "https://api.openai.com/v1",
        "http://api.openai.com/v1",
        "https://api.openai.com.evil.example/v1",
        "https://not-api.openai.com/v1",
        SELF_HOSTED_URL,
        "not a url at all",
        "https://",
    ],
)
def test_the_duplicated_endpoint_predicate_matches_budgets(
    base_url: str | None,
) -> None:
    """The differential oracle the duplication docstring promises.

    ``openai_compatible._official_openai_endpoint`` is a deliberate, temporary
    copy of ``budget._official_openai_endpoint`` -- taken because importing
    ``llm.budget`` here would drag SQLAlchemy and the licensing models into the
    provider's import graph for one predicate. A copy with no oracle comparing
    it to its original is exactly the defect this whole ticket is about, so the
    two are executed side by side rather than assumed equal.

    Stage 2 (CHAOS-3560) deletes the copy; until then this is what stops them
    drifting. Note the ``https://api.openai.com.evil.example`` case: a
    hostname-suffix impostor must be refused by BOTH, and a naive
    ``endswith`` re-implementation of either would admit it.
    """

    from dev_health_ops.llm.agent.openai_compatible import (
        _official_openai_endpoint as agent_predicate,
    )
    from dev_health_ops.llm.budget import _official_openai_endpoint as budget_predicate

    assert agent_predicate(base_url) == budget_predicate(base_url), (
        f"endpoint predicates disagree on {base_url!r}"
    )


# --------------------------------------------------------------------------
# The guard at the construction site, observed refusing.
# --------------------------------------------------------------------------


def _candidate(model: str, *, provider: str = "openai", base_url: str = ""):
    from dev_health_ops.llm.agent.policy import (
        AgentProviderCandidate,
        AgentProviderSource,
    )
    from dev_health_ops.llm.credentials import LLMCredentials

    return AgentProviderCandidate(
        provider=provider,
        model=model,
        credentials=LLMCredentials(api_key="test-key", base_url=base_url),
        source=AgentProviderSource.PLATFORM,
    )


def test_an_unpriced_openai_model_is_booked_LOUDLY_not_refused(caplog) -> None:
    """The invariant is "never SILENTLY book the reservation" -- loud booking satisfies it.

    An earlier revision refused construction. Measured availability evidence
    revised that (team-lead ruling): the price book has three entries, so
    refusing would have taken Ask Dev away from every organization running
    gpt-4o, gpt-5, o3 or anything else unlisted -- a far larger harm than an
    overstated allowance. Platform runs spend real operator dollars, so the
    conservative charge stays; what changes is that it is attributed.

    THE LOUDNESS IS THE GUARD, so it is what this asserts. Both signals are
    checked: a warning naming the model and the remedy, and the metric an
    operator would actually alert on.
    """

    import logging

    from dev_health_ops.api.dev.production_runtime import _provider
    from dev_health_ops.llm.agent.openai_compatible import _estimated_cost_microusd

    with caplog.at_level(logging.WARNING):
        assert _provider(_candidate("gpt-6-imaginary")) is not None

    records = [
        r for r in caplog.records if r.message == "ask_dev.platform_model_unpriced"
    ]
    assert len(records) == 1, "the unpriced model was booked SILENTLY"
    assert getattr(records[0], "model", None) == "gpt-6-imaginary"
    assert "LLM_MODEL" in getattr(records[0], "remedy", "")

    # And the cost really is unknown, so the worst-case reservation stands --
    # the charge this warning is telling the operator about.
    assert (
        _estimated_cost_microusd(
            model="gpt-6-imaginary", input_tokens=10_000, output_tokens=1_000
        )
        is None
    )


def test_self_hosted_books_no_reservation_at_all() -> None:
    """The other half of the invariant, which an earlier revision only claimed.

    ``UNMETERED_SELF_HOSTED`` previously just declined to raise --
    ``_estimated_cost_microusd`` still returned ``None``, so self-hosted
    deployments kept booking US$4/run for hardware the operator already owns.
    The docstring said "must not book the reservation either" and the code did
    not do it. Explicit **0** now, not ``None``: zero reconciles the admission
    reservation away, ``None`` leaves it standing, and that difference is the
    entire defect.
    """

    from dev_health_ops.llm.agent.openai_compatible import _estimated_cost_microusd

    cost = _estimated_cost_microusd(
        model="llama-3.1-70b",
        input_tokens=10_000,
        output_tokens=1_000,
        base_url=SELF_HOSTED_URL,
        provider="ollama",
    )
    assert cost == 0, "self-hosted must be unmetered, not unknown"
    assert cost is not None, "None would leave the US$1 reservation booked"


def test_a_priced_model_still_reports_a_real_cost() -> None:
    """Control: making self-hosted zero must not zero out real metering."""

    from dev_health_ops.llm.agent.openai_compatible import _estimated_cost_microusd

    assert (
        _estimated_cost_microusd(
            model=DEV_STACK_MODEL, input_tokens=10_000, output_tokens=1_000
        )
        == 900
    )


@pytest.mark.parametrize(
    ("model", "provider", "base_url"),
    [
        # priced production model
        (DEV_STACK_MODEL, "openai", ""),
        # the acceptance fixture -- the stack this must not take down
        (SCRIPTED_FIXTURE_MODEL, "openai", ""),
        # self-hosted, unpriceable by design
        ("llama-3.1-70b", "ollama", SELF_HOSTED_URL),
    ],
)
def test_construction_admits_everything_that_is_not_a_misconfiguration(
    model: str, provider: str, base_url: str
) -> None:
    """The guard must be narrow: only the genuine fault raises.

    A guard that also refused the fixture or self-hosted would be worse than
    no guard -- it would trade a wrong number for an outage, and it would take
    the acceptance stack with it.
    """

    from dev_health_ops.api.dev.production_runtime import _provider

    assert (
        _provider(_candidate(model, provider=provider, base_url=base_url)) is not None
    )


def test_the_unpriced_warning_also_increments_the_operator_metric(monkeypatch) -> None:
    """The metric is a separate signal from the log, so it needs its own assertion.

    Caught by the mutation sweep: replacing the ``.inc()`` with ``pass`` left
    every test green, because the loudness test above only read the log. An
    operator alerts on the counter, not on grepping warnings, so a dropped
    metric is a dropped guard -- and the commit message claimed this was
    covered when it was not.
    """

    from dev_health_ops.api.dev import production_runtime

    seen: list[str] = []

    class _Spy:
        def labels(self, **kwargs: str) -> _Spy:
            seen.append(kwargs["model"])
            return self

        def inc(self) -> None:
            seen.append("inc")

    monkeypatch.setattr(
        production_runtime, "ASK_DEV_PLATFORM_MODEL_UNPRICED_TOTAL", _Spy()
    )
    production_runtime._provider(_candidate("gpt-6-imaginary"))

    assert seen == ["gpt-6-imaginary", "inc"]


def test_the_provider_threads_its_own_endpoint_into_the_cost_lookup() -> None:
    """The classification is worthless if the call site drops ``base_url``.

    Also caught by the sweep: hard-coding ``base_url=None`` at the call site
    survived every test, because the self-hosted assertions all called
    ``_estimated_cost_microusd`` directly. Driven through the real provider
    instance here, so the wiring itself is covered rather than the function
    it wires to.

    A self-hosted provider must report 0 (unmetered); the same usage payload
    on an official-endpoint provider running a priced model must report a real
    cost. If the call site stops passing the endpoint, the first becomes a
    priced figure for infrastructure we do not bill.
    """

    from dev_health_ops.llm.agent.openai_compatible import (
        OpenAICompatibleAgentProvider,
    )

    # `_normalize_usage` reads ATTRIBUTES off the SDK's usage object, not dict
    # keys -- a dict silently yields zero tokens and a meaningless zero cost,
    # which would have made this test pass for the wrong reason.
    usage = SimpleNamespace(prompt_tokens=10_000, completion_tokens=1_000)
    response = SimpleNamespace(usage=usage)

    self_hosted = OpenAICompatibleAgentProvider(
        api_key="k",
        model="gpt-5-nano",
        base_url=SELF_HOSTED_URL,
        cost_provider="ollama",
        client=object(),
    )
    official = OpenAICompatibleAgentProvider(
        api_key="k", model="gpt-5-nano", base_url=None, client=object()
    )

    assert self_hosted._normalize_usage(response).estimated_cost_microusd == 0
    assert official._normalize_usage(response).estimated_cost_microusd == 900


@pytest.mark.parametrize(
    ("label", "base_url"),
    [
        ("Azure OpenAI", "https://myco.openai.azure.com/openai/deployments/gpt-5"),
        ("OpenRouter", "https://openrouter.ai/api/v1"),
        ("corporate gateway", "https://llm-gateway.corp.example/v1"),
        ("forwarding proxy", "https://openai-proxy.corp.example/v1"),
    ],
)
def test_a_billable_gateway_is_never_reported_as_free(
    label: str, base_url: str
) -> None:
    """THE fail-open regression control.

    An earlier revision classified self-hosted by URL -- "not api.openai.com"
    implied "free" -- and reported every one of these BILLABLE endpoints at
    cost 0. That is strictly worse than the stuck-reservation overcharge it
    replaced: the overcharge is fail-closed (the org gets throttled early and
    complains), zero is fail-open (real spend accrues against an allowance
    that never moves, and nobody is throttled or told).

    Only the PROVIDER NAME can say "the operator's own hardware". A URL cannot,
    and `openai` + a custom base_url is exactly how Azure and every proxy are
    wired.
    """

    from dev_health_ops.llm.agent.openai_compatible import _estimated_cost_microusd

    assert (
        platform_cost_metering(provider="openai", model="gpt-5-mini", base_url=base_url)
        is PlatformCostMetering.UNKNOWN_BILLABILITY
    ), f"{label} classified as free"
    assert (
        _estimated_cost_microusd(
            model="gpt-5-mini",
            input_tokens=10_000,
            output_tokens=1_000,
            base_url=base_url,
            provider="openai",
        )
        is None
    ), f"{label} reported a cost of zero -- real spend would go unbilled"


def test_only_a_self_hosted_PROVIDER_earns_a_zero_cost() -> None:
    """Zero is licensed by the provider name, never by the URL."""

    from dev_health_ops.llm.agent.openai_compatible import _estimated_cost_microusd

    for provider in ("local", "ollama", "lmstudio"):
        assert (
            _estimated_cost_microusd(
                model="llama-3.1-70b",
                input_tokens=10_000,
                output_tokens=1_000,
                base_url="http://localhost:11434/v1",
                provider=provider,
            )
            == 0
        )


def test_the_classifier_and_the_pricer_agree_on_a_whitespace_model_id() -> None:
    """Two paths that must agree, normalizing differently.

    ``LLM_MODEL="ask-dev-scripted-v1 "`` -- a trailing space in an env var,
    entirely ordinary -- classified as FIXTURE (the carve-out stripped) while
    the price lookup returned None (it did not). The classifier said "carved
    out, proceed" and the pricer left the US$1 reservation standing on every
    call, so the acceptance stack would have drained an allowance while
    reporting itself carved out.

    Found by adversarial review. It is the same defect class as the two price
    books this ticket exists to unify: not a wrong answer, but two answers.
    """

    from dev_health_ops.llm.agent.openai_compatible import _estimated_cost_microusd

    for raw in ("ask-dev-scripted-v1 ", " ask-dev-scripted-v1", "  gpt-5-nano  "):
        classified = platform_cost_metering(provider="openai", model=raw, base_url=None)
        priced = _estimated_cost_microusd(
            model=raw, input_tokens=10_000, output_tokens=1_000
        )
        assert classified is not PlatformCostMetering.UNPRICED_CONFIGURATION_ERROR
        assert priced is not None, (
            f"{raw!r} classified {classified.value} but priced None -- the "
            "reservation would stand on a model the classifier admitted"
        )


def test_the_provider_threads_base_url_so_a_gateway_is_not_priced_as_openai() -> None:
    """Dropping ``base_url`` reintroduces the fail-open in a subtler form.

    The provider-identity test above passes ``cost_provider="ollama"``, whose
    zero is licensed by the PROVIDER, so it never exercises ``base_url`` at
    all. A mutation sweep caught that: hard-coding ``base_url=None`` at the
    call site survived every test.

    It matters most for the case the fail-open fix exists for. An Azure or
    gateway deployment is ``cost_provider="openai"`` with a **priced** model
    name; if the endpoint is dropped, it looks official, the price book hits,
    and we report OpenAI's rates for infrastructure billed by someone else --
    a fabricated cost, and low rather than high.
    """

    from dev_health_ops.llm.agent.openai_compatible import (
        OpenAICompatibleAgentProvider,
    )

    response = SimpleNamespace(
        usage=SimpleNamespace(prompt_tokens=10_000, completion_tokens=1_000)
    )
    gateway = OpenAICompatibleAgentProvider(
        api_key="k",
        model="gpt-5-mini",
        base_url="https://myco.openai.azure.com/openai/deployments/gpt-5",
        cost_provider="openai",
        client=object(),
    )

    assert gateway._normalize_usage(response).estimated_cost_microusd is None, (
        "a paid gateway was priced at OpenAI's rates -- base_url was not "
        "threaded into the cost lookup"
    )


def test_the_two_loud_cases_carry_DIFFERENT_remedies(caplog) -> None:
    """One remedy for both would send an operator chasing the wrong fix.

    Also caught by the sweep: collapsing the conditional so both cases emit
    the price-entry remedy left every test green. A typo'd model needs a price
    added; an Azure/OpenRouter deployment needs its gateway priced
    (CHAOS-3560) and cannot act on "add it to _PLATFORM_MODEL_PRICES" at all.
    """

    import logging

    from dev_health_ops.api.dev.production_runtime import _provider

    def _remedy_and_reason(model: str, base_url: str) -> tuple[str, str]:
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            _provider(_candidate(model, base_url=base_url))
        record = next(
            r for r in caplog.records if r.message == "ask_dev.platform_model_unpriced"
        )
        return getattr(record, "remedy", ""), getattr(record, "reason", "")

    unpriced_remedy, unpriced_reason = _remedy_and_reason("gpt-6-imaginary", "")
    gateway_remedy, gateway_reason = _remedy_and_reason(
        "gpt-5-mini", "https://openrouter.ai/api/v1"
    )

    assert unpriced_reason != gateway_reason
    assert unpriced_remedy != gateway_remedy, "both loud cases emitted the same remedy"
    assert "LLM_MODEL" in unpriced_remedy
    assert "3560" in gateway_remedy or "endpoint" in gateway_remedy
