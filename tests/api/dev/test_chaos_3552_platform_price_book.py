"""CHAOS-3552: an unpriced platform model must fail loudly, not book the reservation.

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

* ``openai`` on an official endpoint with an unpriced model -- a genuine
  operator misconfiguration. **Fail loud at construction.**
* anything self-hosted (``local``/``ollama``/``lmstudio``, or a non-official
  endpoint) -- genuinely unpriceable in dollars, and correctly so. Must **not**
  fail construction, and must **not** book the reservation either.
* ``ask-dev-scripted-v1`` -- a deterministic test double, not a production
  model. Carved out explicitly, keyed to the exact id, and guarded below
  against widening.
"""

from __future__ import annotations

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
        # openai-the-client-library pointed at someone else's endpoint is
        # still self-hosted for pricing purposes -- the rates are not ours to
        # assume. This is the case `budget.reliable_price` already refuses and
        # the agent price book does not.
        ("openai", SELF_HOSTED_URL),
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
        is PlatformCostMetering.UNMETERED_SELF_HOSTED
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
