"""CHAOS-3389 shadow phase: the Question Understanding Agent (QUA) shadow seam.

RED-first coverage for the greenlit shadow-mode-only scope: (1) the seam is
flag-gated and, when the flag is off OR the shadow provider is unavailable,
the live ``OrchestratorResult`` is byte-identical to the seam not existing
at all; (2) the never-widen invariant holds even against a provider that
proposes an out-of-shortlist candidate; (3) an org-wide cardinality
proposal is recorded as uncorroborated unless the deterministic interpreter
independently agrees; (4) the shortlist fetch is authorized through the
SAME ``permission_fingerprint``-keyed boundary the deterministic resolver
uses, never a new one.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import Cardinality
from dev_health_ops.api.dev.qua_shadow import (
    QUAShadowConfig,
    QUAShadowStatus,
    QuestionUnderstandingShadow,
)
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.scope_service import (
    ScopeRequestCache,
    ScopeResolutionService,
)
from dev_health_ops.api.dev.subject_preflight import PreflightDecision
from dev_health_ops.llm.agent.contracts import (
    AgentDecisionResult,
    AgentFinalAnswer,
    AgentMessage,
    AgentProviderCapabilities,
    AgentToolDefinition,
    CancellationSignal,
    StreamingMode,
    StructuredOutputMode,
    ToolDecisionMode,
)
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.scripted import ScriptedAgentProvider, ScriptedStep
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    NIGHTFALL_PROJECT,
    ORG_ID,
    PERMISSION_FINGERPRINT,
    SeededCatalog,
    request_for,
    run_preflight_orchestrator,
)

pytestmark = pytest.mark.asyncio

#: A structurally-valid (if unused) capabilities value -- every fake
#: provider below only ever raises from `decide()`, but must still satisfy
#: the real `AgentLLMProvider` Protocol's `capabilities` property type for
#: mypy, not just duck-type it for pytest.
_FAKE_CAPABILITIES = AgentProviderCapabilities(
    structured_output=StructuredOutputMode.JSON_SCHEMA,
    tool_decisions=ToolDecisionMode.NATIVE,
    streaming=StreamingMode.BUFFERED,
    supports_cancellation=True,
    context_window_tokens=100_000,
    max_output_tokens=10_000,
    readiness_version="test-fake",
    disclosure_key="test-fake",
)


class _RaisingProvider:
    """A shadow provider whose `decide()` always raises a fixed error --
    parametrized rather than duplicated per error kind, and structurally
    typed to satisfy `AgentLLMProvider` for both pytest and mypy."""

    def __init__(self, error: AgentProviderError) -> None:
        self._error = error

    @property
    def capabilities(self) -> AgentProviderCapabilities:
        return _FAKE_CAPABILITIES

    async def decide(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[AgentToolDefinition],
        response_schema: Mapping[str, Any],
        timeout_seconds: float,
        max_output_tokens: int,
        signal: CancellationSignal | None = None,
    ) -> AgentDecisionResult:
        raise self._error

    async def aclose(self) -> None:
        return None


def _catalog(entities) -> SeededCatalog:
    return SeededCatalog(entities)


async def _interpretation(question: str):
    interpreter = QuestionInterpreter()
    return await interpreter.interpret(request_for(question))


def _qua_response(
    *,
    intent_id: str = "entity_status",
    cardinality: str = "singular",
    mentions: list[dict],
    requires_clarification: bool = False,
) -> dict:
    return {
        "schema_version": "dev_question_understanding.v1",
        "intent_id": intent_id,
        "cardinality": cardinality,
        "mentions": mentions,
        "requires_clarification": requires_clarification,
    }


# ---------------------------------------------------------------------------
# RED: flipping the flag changes zero live-path behavior
# ---------------------------------------------------------------------------


def _always_failing_provider() -> _RaisingProvider:
    """A shadow provider that always raises -- proves the live path survives
    a fully broken QUA call, not merely an absent one."""

    return _RaisingProvider(
        AgentProviderError(AgentProviderErrorCode.PROVIDER_UNAVAILABLE)
    )


def _shadow_records(output) -> list:
    """``RunOutput.recorder`` is typed ``Recorder | None`` (a generic
    harness field other callers legitimately never set); every call site in
    this module DOES set it via ``recorder_factory``, so this is a real
    invariant of how this module's tests are built, not an unchecked
    assumption -- asserted here once instead of at each of the several call
    sites below."""

    assert output.recorder is not None
    return output.recorder.qua_shadow_records


async def _shadow(*, enabled: bool, provider) -> QuestionUnderstandingShadow:
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    return QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=enabled),
    )


async def test_shadow_unwired_matches_shadow_wired_but_disabled() -> None:
    """Two different ways to be "off": the seam not constructed at all
    (``qua_shadow=None``, production's own flag-off shape) vs constructed
    with ``config.enabled=False`` (a deployed-but-toggled-off shape). Both
    must leave the live outcome identical; only the disabled-but-wired shape
    additionally records a SKIPPED_DISABLED audit row -- which is itself
    proof the seam ran and chose to do nothing, not proof of a live effect.
    """

    baseline = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-off-baseline",
    )
    shadow_unset = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-off-unset",
        qua_shadow=None,
    )
    shadow_disabled = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-off-disabled",
        qua_shadow=await _shadow(enabled=False, provider=_always_failing_provider()),
    )
    assert shadow_unset.outcome_tuple() == baseline.outcome_tuple()
    assert shadow_disabled.outcome_tuple() == baseline.outcome_tuple()
    assert _shadow_records(shadow_unset) == []
    [record] = _shadow_records(shadow_disabled)
    assert record.status is QUAShadowStatus.SKIPPED_DISABLED
    assert record.latency_ms == 0.0


async def test_shadow_enabled_but_provider_always_fails_is_byte_identical_for_a_proceed_run() -> (
    None
):
    """The stronger claim: even a WORKING, ENABLED shadow seam whose provider
    always raises must never change the live outcome -- only its own record."""

    baseline = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-fail-baseline",
    )
    shadow_failing = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-fail-shadow",
        qua_shadow=await _shadow(enabled=True, provider=_always_failing_provider()),
    )
    assert shadow_failing.outcome_tuple() == baseline.outcome_tuple()
    # The shadow record itself IS observable -- proving the seam ran -- it
    # just never touched the live outcome above.
    assert len(_shadow_records(shadow_failing)) == 1
    assert (
        _shadow_records(shadow_failing)[0].status
        is QUAShadowStatus.SKIPPED_PROVIDER_ERROR
    )


async def test_shadow_enabled_but_provider_always_fails_is_byte_identical_for_a_terminate_run() -> (
    None
):
    """Same proof on the OTHER decision branch (TERMINATE): an unresolved
    named subject's clarification terminal is unaffected by a failing
    shadow seam."""

    baseline = await run_preflight_orchestrator(
        question="What's the status of the Nightfall project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-terminate-baseline",
    )
    shadow_failing = await run_preflight_orchestrator(
        question="What's the status of the Nightfall project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-terminate-shadow",
        qua_shadow=await _shadow(enabled=True, provider=_always_failing_provider()),
    )
    assert shadow_failing.outcome_tuple() == baseline.outcome_tuple()
    assert baseline.result.state is not None
    assert (
        shadow_failing.result.answer is None
    )  # sanity: this really is a TERMINATE run


async def test_shadow_enabled_with_a_successful_proposal_still_does_not_change_the_answer() -> (
    None
):
    """The strongest version of the RED proof: the shadow provider not only
    runs, it returns a VALID, high-confidence, correct-looking proposal --
    and the live answer is still byte-identical to the flag being off."""

    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    mention = interpretation.mentions[0]
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        mentions=[
                            {
                                "text_span": mention.original_text_span,
                                "outcome": "resolved",
                                "selected_candidate_index": 0,
                                "candidate_indices": [0],
                                "confidence": 0.99,
                            }
                        ]
                    )
                )
            )
        ],
        script_id="qua-success",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )

    baseline = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-success-baseline",
    )
    shadowed = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-success-live",
        qua_shadow=shadow,
    )
    assert shadowed.outcome_tuple() == baseline.outcome_tuple()
    [record] = _shadow_records(shadowed)
    assert record.status is QUAShadowStatus.EVALUATED
    assert record.mentions[0].selected_entity == ASK_DEV_PROJECT
    assert record.mentions[0].rejected_reason is None


class _RaisingShadow:
    """Not a real ``QuestionUnderstandingShadow`` -- duck-typed to bypass
    its own internal exception handling entirely and raise straight out of
    ``evaluate()``, the way a genuine bug in qua_shadow.py itself would.
    Proves the ORCHESTRATOR's own defensive try/except (orchestrator.py's
    call site, distinct from qua_shadow.py's internal one every other test
    in this module exercises) is load-bearing, not merely decorative."""

    async def evaluate(self, **_kwargs):
        raise RuntimeError("qua_shadow.py has a real bug")


async def test_a_shadow_component_that_raises_outright_still_never_affects_the_run() -> (
    None
):
    baseline = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-raising-baseline",
    )
    with_raising_shadow = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-raising-live",
        qua_shadow=_RaisingShadow(),
    )
    assert with_raising_shadow.outcome_tuple() == baseline.outcome_tuple()
    # evaluate() raised before ever returning a record -- nothing to persist.
    assert _shadow_records(with_raising_shadow) == []


# ---------------------------------------------------------------------------
# Never-widen holds structurally, even against a malicious/buggy provider
# ---------------------------------------------------------------------------


async def test_an_index_outside_the_mentions_own_shortlist_is_rejected_not_trusted() -> (
    None
):
    """Two mentions, each with its own disjoint shortlist. A provider that
    (however schema-valid the integer is for the call as a whole) selects
    an index that belonged to the OTHER mention's shortlist must never have
    that candidate recorded as selected."""

    catalog = _catalog(
        [
            (ORG_ID, ATLAS_PROJECT_ONE),
            (ORG_ID, ATLAS_PROJECT_TWO),
            (ORG_ID, NIGHTFALL_PROJECT),
        ]
    )
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation(
        "What's the status of the Atlas project and the Nightfall project?"
    )
    assert len(interpretation.mentions) == 2

    # Atlas resolves ambiguous (two entities) -> occupies indices [0, 1];
    # Nightfall resolves exact -> occupies index [2]. A provider claiming
    # index 2 for the FIRST (Atlas) mention is claiming a candidate that was
    # only ever authorized for the second mention.
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        cardinality="plural_cohort",
                        mentions=[
                            {
                                "text_span": interpretation.mentions[
                                    0
                                ].original_text_span,
                                "outcome": "resolved",
                                "selected_candidate_index": 2,
                                "candidate_indices": [],
                                "confidence": 0.9,
                            },
                            {
                                "text_span": interpretation.mentions[
                                    1
                                ].original_text_span,
                                "outcome": "resolved",
                                "selected_candidate_index": 2,
                                "candidate_indices": [],
                                "confidence": 0.9,
                            },
                        ],
                    )
                )
            )
        ],
        script_id="qua-cross-mention",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Atlas project and the Nightfall project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.TERMINATE,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.EVALUATED
    first, second = record.mentions
    assert first.selected_entity is None
    assert first.rejected_reason == "index_outside_mention_shortlist"
    # The second mention legitimately owns index 2 -- its own proposal is
    # unaffected by the first mention's rejection.
    assert second.selected_entity == NIGHTFALL_PROJECT
    assert second.rejected_reason is None


async def test_zero_authorized_candidates_makes_every_index_structurally_inexpressible() -> (
    None
):
    """When a mention has no authorized candidates at all, the wire schema's
    own bound is [0, -1] -- no integer satisfies it. A provider that ignores
    the schema and returns index 0 anyway is still caught by the runtime
    verifier."""

    catalog = _catalog([])  # nothing authorized in this org at all
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        mentions=[
                            {
                                "text_span": interpretation.mentions[
                                    0
                                ].original_text_span,
                                "outcome": "resolved",
                                "selected_candidate_index": 0,
                                "candidate_indices": [0],
                                "confidence": 0.9,
                            }
                        ]
                    )
                )
            )
        ],
        script_id="qua-empty-shortlist",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.TERMINATE,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.EVALUATED
    [assessment] = record.mentions
    assert assessment.selected_entity is None
    assert assessment.rejected_reason == "index_outside_mention_shortlist"


# ---------------------------------------------------------------------------
# Org-wide cardinality requires deterministic corroboration
# ---------------------------------------------------------------------------


async def test_org_wide_proposal_uncorroborated_when_deterministic_named_a_subject() -> (
    None
):
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    assert interpretation.intent.cardinality is not Cardinality.ORGANIZATION_WIDE
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        cardinality="organization_wide",
                        mentions=[
                            {
                                "text_span": interpretation.mentions[
                                    0
                                ].original_text_span,
                                "outcome": "no_match",
                                "selected_candidate_index": None,
                                "candidate_indices": [],
                                "confidence": 0.4,
                            }
                        ],
                    )
                )
            )
        ],
        script_id="qua-org-wide-uncorroborated",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.TERMINATE,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.EVALUATED
    assert record.cardinality is Cardinality.ORGANIZATION_WIDE
    assert record.cardinality_corroborated is False


async def test_org_wide_proposal_corroborated_when_deterministic_also_computed_org_wide() -> (
    None
):
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    # A genuinely org-wide question, per CHAOS-3393's own portfolio case:
    # no named mentions at all.
    interpreter = QuestionInterpreter()
    interpretation = await interpreter.interpret(
        request_for("How is the whole portfolio doing?")
    )
    assert interpretation.intent.cardinality is Cardinality.ORGANIZATION_WIDE
    assert not interpretation.mentions

    async def one() -> None:
        # evaluate() itself skips when there are no mentions -- the
        # cardinality-corroboration helper is exercised directly here since
        # a mentionless call never reaches the model at all (correctly: an
        # org-wide question is not a resolution question).
        shadow = QuestionUnderstandingShadow(
            provider=ScriptedAgentProvider([], script_id="unused"),
            scope_service=scope_service,
            config=QUAShadowConfig(enabled=True),
        )
        assert shadow._cardinality_corroborated(  # noqa: SLF001 - internal, tested directly
            interpretation=interpretation, proposed=Cardinality.ORGANIZATION_WIDE
        )

    await one()


# ---------------------------------------------------------------------------
# Silent-skip on every "not available" condition -- never a block
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("config", "provider", "expected"),
    [
        (
            QUAShadowConfig(enabled=False),
            "present",
            QUAShadowStatus.SKIPPED_DISABLED,
        ),
        (
            QUAShadowConfig(enabled=True),
            None,
            QUAShadowStatus.SKIPPED_NO_PROVIDER,
        ),
    ],
)
async def test_unavailable_conditions_skip_silently_with_a_typed_status(
    config, provider, expected
) -> None:
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    resolved_provider = (
        ScriptedAgentProvider([], script_id="unused") if provider == "present" else None
    )
    shadow = QuestionUnderstandingShadow(
        provider=resolved_provider, scope_service=scope_service, config=config
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is expected
    assert record.latency_ms == 0.0


async def test_no_mentions_skips_silently() -> None:
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await QuestionInterpreter().interpret(
        request_for("How is the whole portfolio doing?")
    )
    shadow = QuestionUnderstandingShadow(
        provider=ScriptedAgentProvider([], script_id="unused"),
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="How is the whole portfolio doing?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.SKIPPED_NO_MENTIONS


async def test_zero_remaining_budget_skips_without_calling_the_provider() -> None:
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    shadow = QuestionUnderstandingShadow(
        provider=_always_failing_provider(),
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=0.0,
    )
    assert record.status is QUAShadowStatus.SKIPPED_BUDGET_EXHAUSTED
    assert record.latency_ms == 0.0


async def test_malformed_output_is_recorded_as_invalid_not_raised() -> None:
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    provider = ScriptedAgentProvider(
        [ScriptedStep(decision=AgentFinalAnswer({"not": "the right shape"}))],
        script_id="qua-malformed",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.SKIPPED_INVALID_OUTPUT


# ---------------------------------------------------------------------------
# permission_fingerprint hardening: reuses the existing authorized-catalog
# cache boundary, never a new one.
# ---------------------------------------------------------------------------


async def test_shortlist_fetch_is_authorized_through_the_permission_fingerprint_boundary() -> (
    None
):
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    shadow = QuestionUnderstandingShadow(
        provider=ScriptedAgentProvider([], script_id="unused"),
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    per_mention = await shadow._shortlist(  # noqa: SLF001 - internal, tested directly
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
    )
    assert per_mention[interpretation.mentions[0].mention_id] == (ASK_DEV_PROJECT,)
    # The search actually went through scope_service.search -> the seeded
    # catalog's own search(), which is the SAME call the deterministic
    # resolver makes -- proven by the catalog's own recorded call.
    assert any(org == ORG_ID for org, _query in catalog.search_calls)


# ---------------------------------------------------------------------------
# Codex adversarial review round 1 findings, each with a RED test proving
# the fix -- these fail against the pre-fix code (verified by hand before
# landing the fix, per this repo's RED-first convention).
# ---------------------------------------------------------------------------


async def test_a_provider_internal_timeout_maps_to_skipped_timeout_not_generic_error() -> (
    None
):
    """Codex round 1 (MEDIUM, confirmed): every AgentProviderError used to
    collapse onto SKIPPED_PROVIDER_ERROR, hiding the provider's own
    TIMEOUT code behind the same generic bucket the RED test in this module
    already uses for a genuine provider failure -- corrupting shadow
    telemetry the eventual probe-certification needs to be trustworthy.

    Simulates the real OpenAI-compatible adapter's OWN internal timeout
    race: raises ``AgentProviderError(TIMEOUT)`` directly, the way a
    provider that lost its own wait_for race against ``timeout_seconds``
    does -- never via this method's outer ``asyncio.wait_for`` at all.
    """

    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    shadow = QuestionUnderstandingShadow(
        provider=_RaisingProvider(
            AgentProviderError(AgentProviderErrorCode.TIMEOUT, retryable=True)
        ),
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.SKIPPED_TIMEOUT
    assert record.latency_ms > 0.0


async def test_a_non_timeout_provider_error_keeps_its_own_code_as_error_class() -> None:
    """The companion case: a RATE_LIMITED (or any other non-timeout)
    AgentProviderError stays under SKIPPED_PROVIDER_ERROR, but now with the
    specific code as `error_class` instead of the uninformative literal
    "AgentProviderError" every kind used to share."""

    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    shadow = QuestionUnderstandingShadow(
        provider=_RaisingProvider(
            AgentProviderError(AgentProviderErrorCode.RATE_LIMITED, retryable=True)
        ),
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.SKIPPED_PROVIDER_ERROR
    assert record.error_class == "rate_limited"


@pytest.mark.parametrize(
    "code",
    [
        AgentProviderErrorCode.BUDGET_EXHAUSTED,
        AgentProviderErrorCode.BUDGET_UNAVAILABLE,
    ],
)
async def test_the_isolated_shadow_quota_running_out_is_a_typed_budget_skip(
    code: AgentProviderErrorCode,
) -> None:
    """CHAOS-3452: ``attach_qua_shadow_budget_guard`` (llm/qua_shadow_budget.py)
    raises exactly these two codes when the ISOLATED shadow quota -- never
    the live BYO budget -- is exhausted or its accounting fails. Both must
    read as the seam's OWN typed budget skip (``SKIPPED_BUDGET_EXHAUSTED``,
    previously only reachable via the wall-clock deadline check), not fall
    into the generic ``SKIPPED_PROVIDER_ERROR`` bucket every other
    AgentProviderError kind uses."""

    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    shadow = QuestionUnderstandingShadow(
        provider=_RaisingProvider(AgentProviderError(code)),
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.SKIPPED_BUDGET_EXHAUSTED
    assert record.error_class == code.value


async def test_isolated_shadow_quota_exhaustion_is_byte_identical_for_a_proceed_run() -> (
    None
):
    """The orchestrator-level companion to the unit test above: even a
    shadow provider that always fails with the isolated quota's own
    BUDGET_EXHAUSTED code -- exactly what a real exhausted
    ``dev_qua_shadow_budget_reservations`` pool raises -- leaves the live
    ``OrchestratorResult`` byte-identical to the flag being off."""

    baseline = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-shadow-quota-baseline",
    )
    shadow_quota_exhausted = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-shadow-quota-exhausted",
        qua_shadow=await _shadow(
            enabled=True,
            provider=_RaisingProvider(
                AgentProviderError(AgentProviderErrorCode.BUDGET_EXHAUSTED)
            ),
        ),
    )
    assert shadow_quota_exhausted.outcome_tuple() == baseline.outcome_tuple()
    [record] = _shadow_records(shadow_quota_exhausted)
    assert record.status is QUAShadowStatus.SKIPPED_BUDGET_EXHAUSTED
    assert record.error_class == "budget_exhausted"


async def test_resolved_outcome_without_an_index_is_rejected_not_evaluated_clean() -> (
    None
):
    """Codex round 1 (MEDIUM, confirmed): outcome and
    selected_candidate_index are independently-typed on the wire -- a
    provider proposing "resolved" with no index (or with one rejected by
    the shortlist-bounds check) must not persist as clean EVALUATED
    evidence with a None selection and no explanation."""

    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        mentions=[
                            {
                                "text_span": interpretation.mentions[
                                    0
                                ].original_text_span,
                                "outcome": "resolved",
                                "selected_candidate_index": None,
                                "candidate_indices": [],
                                "confidence": 0.8,
                            }
                        ]
                    )
                )
            )
        ],
        script_id="qua-resolved-no-index",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.EVALUATED
    [assessment] = record.mentions
    assert assessment.rejected_reason == "resolved_outcome_missing_index"


async def test_no_match_outcome_with_an_index_is_rejected_and_the_index_dropped() -> (
    None
):
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        mentions=[
                            {
                                "text_span": interpretation.mentions[
                                    0
                                ].original_text_span,
                                "outcome": "no_match",
                                "selected_candidate_index": 0,
                                "candidate_indices": [],
                                "confidence": 0.2,
                            }
                        ]
                    )
                )
            )
        ],
        script_id="qua-no-match-with-index",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.EVALUATED
    [assessment] = record.mentions
    assert assessment.rejected_reason == "non_resolved_outcome_has_index"
    assert assessment.selected_entity is None


class _SlowSearchCatalog(SeededCatalog):
    """Same seeded data, but ``search`` never returns within any
    real-world shadow budget -- simulates a hanging/very slow catalog."""

    def __init__(self, *args, delay_seconds: float, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._delay_seconds = delay_seconds

    async def search(self, *args, **kwargs):
        import asyncio as _asyncio

        await _asyncio.sleep(self._delay_seconds)
        return await super().search(*args, **kwargs)


async def test_a_hanging_catalog_search_is_bounded_by_the_shadow_budget_not_uncapped() -> (
    None
):
    """Codex round 1 (HIGH, confirmed): the shadow budget used to be
    computed once, up front, and only ever enforced on the provider call --
    catalog search itself was a bare, uncapped `asyncio.gather`. Because
    `evaluate()` is awaited synchronously in the orchestrator's own
    critical path, an unbounded catalog call would delay or hang the live
    run it is supposed to never affect. Proven here with a catalog that
    sleeps for far longer than the configured hard timeout -- the WHOLE
    call must still return within roughly that budget, not the sleep
    duration."""

    import time

    catalog = _SlowSearchCatalog([(ORG_ID, ASK_DEV_PROJECT)], delay_seconds=5.0)
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    shadow = QuestionUnderstandingShadow(
        provider=_always_failing_provider(),
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True, hard_timeout_seconds=0.2),
    )
    started = time.monotonic()
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    elapsed = time.monotonic() - started
    assert elapsed < 1.0, (
        f"evaluate() took {elapsed:.2f}s against a 0.2s hard timeout and a "
        "5s catalog delay -- the catalog call is not actually bounded"
    )
    assert record.status is QUAShadowStatus.SKIPPED_CATALOG_UNAVAILABLE


# ---------------------------------------------------------------------------
# Codex adversarial review round 2 findings.
# ---------------------------------------------------------------------------


async def test_a_bool_masquerading_as_an_index_is_rejected_not_silently_coerced() -> (
    None
):
    """Codex round 2 (MEDIUM, confirmed): pydantic's default lax mode
    coerces ``True`` to ``1`` for an ``int`` field (``bool`` is an ``int``
    subclass in Python) -- a provider response with
    ``selected_candidate_index: true`` would otherwise validate as a clean,
    confident selection of candidate 1 instead of being rejected as
    malformed output."""

    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        mentions=[
                            {
                                "text_span": interpretation.mentions[
                                    0
                                ].original_text_span,
                                "outcome": "resolved",
                                "selected_candidate_index": True,
                                "candidate_indices": [],
                                "confidence": 0.9,
                            }
                        ]
                    )
                )
            )
        ],
        script_id="qua-bool-as-index",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.SKIPPED_INVALID_OUTPUT


async def test_a_numeric_string_confidence_is_rejected_not_silently_coerced() -> None:
    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        mentions=[
                            {
                                "text_span": interpretation.mentions[
                                    0
                                ].original_text_span,
                                "outcome": "no_match",
                                "selected_candidate_index": None,
                                "candidate_indices": [],
                                "confidence": "0.9",
                            }
                        ]
                    )
                )
            )
        ],
        script_id="qua-string-confidence",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.SKIPPED_INVALID_OUTPUT


async def test_the_real_wire_shape_still_validates_cleanly_under_strict_fields() -> (
    None
):
    """Guard against over-correction: `intent_id`/`cardinality` (enums) and
    ordinary well-typed ints/floats must still validate normally -- strict
    mode is scoped to the specific coercion-prone fields only, not the
    whole model (see the contract module's own docstring for why a
    model-wide `strict=True` was tried and reverted)."""

    catalog = _catalog([(ORG_ID, ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    interpretation = await _interpretation("What's the status of the Ask Dev project?")
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        mentions=[
                            {
                                "text_span": interpretation.mentions[
                                    0
                                ].original_text_span,
                                "outcome": "resolved",
                                "selected_candidate_index": 0,
                                "candidate_indices": [0],
                                "confidence": 0.9,
                            }
                        ]
                    )
                )
            )
        ],
        script_id="qua-well-typed",
    )
    shadow = QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True),
    )
    record = await shadow.evaluate(
        question="What's the status of the Ask Dev project?",
        interpretation=interpretation,
        org_id=ORG_ID,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        deterministic_decision=PreflightDecision.PROCEED,
        remaining_seconds=10.0,
    )
    assert record.status is QUAShadowStatus.EVALUATED


async def test_a_broken_recorder_during_shadow_write_recovery_never_degrades_the_run() -> (
    None
):
    """Codex round 2 (HIGH, confirmed): the shadow-write-failure recovery
    sequence (rollback + replay) was itself unguarded -- if THAT also
    raises (a genuinely dead connection, not just a constraint violation on
    the shadow row alone), the exception used to propagate out of this
    whole block to the orchestrator's own top-level catch-all, degrading a
    routine graceful terminal into a generic internal_error. Drives that
    exact double-failure through the real orchestrator via a recorder whose
    `record_qua_shadow` AND `rollback` both raise, and asserts the run
    still reaches its normal terminal -- never internal_error."""

    class _DoublyBrokenRecorder:
        """Wraps a real Recorder-shaped fake; record_qua_shadow AND
        rollback both raise, simulating a connection that is truly gone."""

        def __init__(self, inner) -> None:
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        async def record_qua_shadow(self, record) -> None:
            raise RuntimeError("shadow write: connection gone")

        async def rollback(self) -> None:
            raise RuntimeError("rollback: connection gone too")

    from tests._chaos_3292_preflight import Recorder as _FakeRecorder

    def recorder_factory():
        return _DoublyBrokenRecorder(_FakeRecorder())

    baseline = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-recovery-fault-baseline",
    )
    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="qua-recovery-fault-live",
        qua_shadow=await _shadow(enabled=True, provider=_always_failing_provider()),
        recorder_factory=recorder_factory,
    )
    # The double failure must never surface as the orchestrator's generic
    # catch-all terminal -- the run reaches the SAME outcome the baseline
    # (no shadow seam at all) does.
    assert output.result.state == baseline.result.state
    assert (output.result.error.code if output.result.error else None) != (
        "internal_error"
    )
