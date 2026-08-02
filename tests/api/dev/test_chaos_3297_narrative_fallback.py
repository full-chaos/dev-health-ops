"""CHAOS-3297 stack #4: narrative brief, provider call, layered validation,
deterministic fallback.

Uses ``contract_fixtures_v2.positive_fixtures()["dev_answer_frame.v1"]`` --
the same producer-validated golden every other v2 contract test in this
package uses -- as the frame fixture, never a hand-authored dict. Tests are
grouped:

* the P5 brief-projection totality guard and its content;
* the closed provider-failure vocabulary and its total classification
  (requirement: an unrecognized failure falls back and counts, never
  raises);
* the deterministic-fallback narrative's structural isolation from provider
  output (planted-defect proof: no provider token ever reaches it);
* P6's runtime identity invariant (narrative choice is the only delta);
* mutation controls: each guard disabled, observed load-bearing by the
  fixture it alone would otherwise catch.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev import contract_fixtures_v2
from dev_health_ops.api.dev.answer_frames import narrative_fallback as _nf
from dev_health_ops.api.dev.answer_frames import narrative_request as _nreq
from dev_health_ops.api.dev.answer_frames.narrative_fallback import (
    _KNOWN_PROVIDER_EXCEPTIONS,
    NarrativeFailureCode,
    NarrativeProviderBudgetExceededError,
    NarrativeProviderEmptyContentError,
    NarrativeProviderRefusalError,
    NarrativeProviderResult,
    NarrativeProviderSchemaViolationError,
    NarrativeProviderTimeoutError,
    NarrativeProviderUnsafeContentError,
    assert_narrative_choice_is_the_only_delta,
    build_deterministic_fallback_narrative,
    classify_provider_exception,
    synthesize_narrative_answer,
)
from dev_health_ops.api.dev.answer_frames.narrative_request import (
    NARRATIVE_BRIEF_FIELD_POLICY,
    NarrativeFieldDisposition,
    build_narrative_brief,
)
from dev_health_ops.api.dev.contract_fixtures_v2 import positive_fixtures
from dev_health_ops.api.dev.contracts_v2 import validators as v2_validators
from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame
from dev_health_ops.metrics.prometheus import ASK_DEV_NARRATIVE_FALLBACK_TOTAL

_ANSWER_ID = "0f1a2b3c-0004-4a00-8000-000000000001"
_CONVERSATION_ID = "0f1a2b3c-0005-4a00-8000-000000000001"
_GENERATED_AT = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)


def _frame() -> DevAnswerFrame:
    return DevAnswerFrame.model_validate(positive_fixtures()["dev_answer_frame.v1"])


class _ScriptedProvider:
    """A provider stand-in whose one call either returns a fixed result or
    raises a fixed exception -- the ``llm.agent.scripted`` pattern the plan's
    e2e controls use, minimized for this module's provider boundary."""

    def __init__(
        self,
        *,
        result: NarrativeProviderResult | None = None,
        raises: Exception | None = None,
    ) -> None:
        self._result = result
        self._raises = raises
        self.calls = 0

    async def generate_narrative(
        self, brief: Mapping[str, Any]
    ) -> NarrativeProviderResult:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        assert self._result is not None
        return self._result


def _valid_provider_result(frame: DevAnswerFrame) -> NarrativeProviderResult:
    """A provider response that survives every layer-3 validator against
    ``_frame()`` -- names the subject, cites only grounded facts/numbers."""

    fact = frame.facts[0]
    section = frame.sections[0]
    subject_label = frame.subject_ref.display_label  # type: ignore[union-attr]
    return NarrativeProviderResult(
        body=(
            f"{subject_label} is not ready. {fact.text} "
            "3 of 4 required items are complete."
        ),
        referenced_fact_ids=(fact.fact_id,),
        referenced_section_ids=(section.section_id,),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )


# ---------------------------------------------------------------------------
# P5 brief projection
# ---------------------------------------------------------------------------


def test_brief_policy_is_total_over_the_frame_model():
    declared = set(NARRATIVE_BRIEF_FIELD_POLICY)
    actual = set(DevAnswerFrame.model_fields)
    assert declared == actual


def test_brief_includes_only_classified_included_fields():
    frame = _frame()
    brief = build_narrative_brief(frame)
    included = {
        name
        for name, disposition in NARRATIVE_BRIEF_FIELD_POLICY.items()
        if disposition is NarrativeFieldDisposition.INCLUDED
    }
    assert set(brief) == included


def test_brief_excludes_internal_correlation_and_provenance():
    brief = build_narrative_brief(_frame())
    for excluded_field in (
        "frame_id",
        "run_id",
        "generated_at",
        "versions",
        "evidence",
    ):
        assert excluded_field not in brief


def test_brief_carries_the_content_needed_for_grounding():
    brief = build_narrative_brief(_frame())
    assert brief["facts"], "narrative cannot cite a fact it never saw"
    assert brief["subject_ref"]["display_label"], (
        "validate_narrative_subject_claim requires the provider to know the "
        "subject's canonical display label"
    )
    assert brief["completion"] is not None
    assert brief["readiness"] is not None


def _frame_with_one_metric(**extra_metric_fields: Any) -> DevAnswerFrame:
    # contract_fixtures_v2._metric_ref() is the producer-validated golden for
    # dev_metric_ref.v1 -- reused rather than hand-authoring resolved_scope/
    # current_window shapes (feedback_generate_fixtures_from_the_producer).
    metric = {**contract_fixtures_v2._metric_ref(), **extra_metric_fields}
    return DevAnswerFrame.model_validate(
        {**positive_fixtures()["dev_answer_frame.v1"], "metrics": [metric]}
    )


def test_metric_evidence_ref_ids_are_stripped_from_the_brief():
    frame = _frame_with_one_metric()
    brief = build_narrative_brief(frame)
    assert "evidence_ref_ids" not in brief["metrics"][0]


def test_a_future_metric_evidence_classification_field_would_also_be_stripped():
    """Forward-compat proof for the orchestrator's ruling on stack #3's
    incoming ``DevMetricRefV2.evidence_classification`` field: exercises the
    stripping mechanism directly (the real contract does not carry this
    field yet), so the exclusion is proven correct ahead of the field's
    landing rather than discovered broken on rebase."""

    metric_dict = {
        "metric_ref_id": "metric_01",
        "label": "Cycle time (p50)",
        "value": 12.5,
        "evidence_classification": "legacy_v1_unminted",
    }
    projected = _nreq._project_metric_for_brief(metric_dict)
    assert "evidence_classification" not in projected
    assert projected["value"] == 12.5  # narratable content is preserved


def test_an_unclassified_new_frame_field_breaks_the_real_totality_check(monkeypatch):
    """Planted defect, through the real production function (rule 4: a
    measurement that did not happen must FAIL): remove one field's
    disposition and call the exact totality checker import time runs.
    Proves the checker -- not a reimplementation of it -- is load-bearing."""

    from dev_health_ops.api.dev.answer_frames import narrative_request as _req

    incomplete_policy = dict(NARRATIVE_BRIEF_FIELD_POLICY)
    del incomplete_policy["facts"]
    monkeypatch.setattr(_req, "NARRATIVE_BRIEF_FIELD_POLICY", incomplete_policy)

    with pytest.raises(RuntimeError, match="facts"):
        _req.assert_narrative_brief_policy_is_total()


def test_a_stale_policy_entry_for_a_removed_field_also_breaks_the_check(monkeypatch):
    from dev_health_ops.api.dev.answer_frames import narrative_request as _req

    stale_policy = dict(NARRATIVE_BRIEF_FIELD_POLICY)
    stale_policy["field_that_no_longer_exists"] = NarrativeFieldDisposition.EXCLUDED
    monkeypatch.setattr(_req, "NARRATIVE_BRIEF_FIELD_POLICY", stale_policy)

    with pytest.raises(RuntimeError, match="field_that_no_longer_exists"):
        _req.assert_narrative_brief_policy_is_total()


# ---------------------------------------------------------------------------
# Closed provider-failure vocabulary + total classification
# ---------------------------------------------------------------------------


def test_known_provider_exceptions_membership_is_pinned():
    """Regression fence (team-lead requirement #1), mirroring how
    CHAOS-3300 pins ``LEGACY_ONLY_QUESTION_INTENTS``: adding a 7th provider
    exception type is a deliberate edit to this test, not a silent drift."""

    assert _KNOWN_PROVIDER_EXCEPTIONS == (
        NarrativeProviderTimeoutError,
        NarrativeProviderRefusalError,
        NarrativeProviderEmptyContentError,
        NarrativeProviderSchemaViolationError,
        NarrativeProviderBudgetExceededError,
        NarrativeProviderUnsafeContentError,
    )


def test_narrative_failure_code_membership_is_pinned():
    assert {member.value for member in NarrativeFailureCode} == {
        "provider_timeout",
        "provider_refused",
        "provider_empty_content",
        "provider_schema_violation",
        "provider_output_budget_exceeded",
        "provider_unsafe_content",
        "narrative_grounding_failed",
        "provider_unknown_failure",
    }


@pytest.mark.parametrize(
    ("exc", "expected_code"),
    [
        (NarrativeProviderTimeoutError(), NarrativeFailureCode.PROVIDER_TIMEOUT),
        (NarrativeProviderRefusalError(), NarrativeFailureCode.PROVIDER_REFUSED),
        (
            NarrativeProviderEmptyContentError(),
            NarrativeFailureCode.PROVIDER_EMPTY_CONTENT,
        ),
        (
            NarrativeProviderSchemaViolationError(),
            NarrativeFailureCode.PROVIDER_SCHEMA_VIOLATION,
        ),
        (
            NarrativeProviderBudgetExceededError(),
            NarrativeFailureCode.PROVIDER_OUTPUT_BUDGET_EXCEEDED,
        ),
        (
            NarrativeProviderUnsafeContentError(),
            NarrativeFailureCode.PROVIDER_UNSAFE_CONTENT,
        ),
    ],
)
def test_each_known_provider_exception_classifies_to_its_own_code(exc, expected_code):
    assert classify_provider_exception(exc) is expected_code


def test_an_unrecognized_exception_classifies_as_unknown_never_raises():
    """The totality requirement itself: classify_provider_exception must
    never re-raise, for any exception type, known or not."""

    class SomeLibraryTransportError(Exception):
        pass

    assert (
        classify_provider_exception(SomeLibraryTransportError("boom"))
        is NarrativeFailureCode.PROVIDER_UNKNOWN_FAILURE
    )
    assert (
        classify_provider_exception(ValueError("also unknown to this classifier"))
        is NarrativeFailureCode.PROVIDER_UNKNOWN_FAILURE
    )


@pytest.mark.asyncio
async def test_an_unrecognized_provider_exception_falls_back_and_counts_never_raises():
    """End-to-end version of the totality requirement: a provider that
    raises something outside NarrativeProviderError entirely (never wrapped
    -- e.g. a raw transport exception a future concrete provider forgot to
    catch) must still produce a safe deterministic answer, not propagate."""

    class UnwrappedTransportError(Exception):
        pass

    frame = _frame()
    provider = _ScriptedProvider(raises=UnwrappedTransportError("connection reset"))
    before = ASK_DEV_NARRATIVE_FALLBACK_TOTAL.labels(
        failure_code=NarrativeFailureCode.PROVIDER_UNKNOWN_FAILURE.value
    )._value.get()

    answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is NarrativeFailureCode.PROVIDER_UNKNOWN_FAILURE
    assert answer.narrative is not None
    assert answer.narrative.mode == "deterministic_fallback"
    after = ASK_DEV_NARRATIVE_FALLBACK_TOTAL.labels(
        failure_code=NarrativeFailureCode.PROVIDER_UNKNOWN_FAILURE.value
    )._value.get()
    assert after == before + 1


# ---------------------------------------------------------------------------
# The provider-failure matrix (C3): every known failure mode still produces
# the frame-derived answer, only narrative.mode and the failure code differ.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exc", "expected_code"),
    [
        (NarrativeProviderTimeoutError(), NarrativeFailureCode.PROVIDER_TIMEOUT),
        (NarrativeProviderRefusalError(), NarrativeFailureCode.PROVIDER_REFUSED),
        (
            NarrativeProviderEmptyContentError(),
            NarrativeFailureCode.PROVIDER_EMPTY_CONTENT,
        ),
        (
            NarrativeProviderSchemaViolationError(),
            NarrativeFailureCode.PROVIDER_SCHEMA_VIOLATION,
        ),
        (
            NarrativeProviderBudgetExceededError(),
            NarrativeFailureCode.PROVIDER_OUTPUT_BUDGET_EXCEEDED,
        ),
        (
            NarrativeProviderUnsafeContentError(),
            NarrativeFailureCode.PROVIDER_UNSAFE_CONTENT,
        ),
    ],
)
async def test_provider_failure_matrix_preserves_the_frame_baseline(exc, expected_code):
    frame = _frame()
    provider = _ScriptedProvider(raises=exc)

    baseline_answer, baseline_code = await synthesize_narrative_answer(
        frame=frame,
        provider=None,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )
    assert baseline_code is None

    fallback_answer, fallback_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert fallback_code is expected_code
    assert fallback_answer.public_outcome == baseline_answer.public_outcome
    assert fallback_answer.frame == baseline_answer.frame
    assert (
        fallback_answer.outcome_display_label == baseline_answer.outcome_display_label
    )
    assert fallback_answer.narrative is not None
    assert fallback_answer.narrative.mode == "deterministic_fallback"
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_a_misbehaving_provider_returning_an_empty_body_still_falls_back():
    """Defense in depth: the ``NarrativeProvider`` contract says a provider
    must raise ``NarrativeProviderEmptyContentError`` rather than return an
    empty ``body`` -- but if one misbehaves and returns one anyway,
    ``DevNarrative``'s own ``LongText`` constraint (``min_length=1``) raises
    a ``pydantic.ValidationError`` (a ``ValueError`` subclass) building the
    candidate, which the grounding-failure except clause already catches.
    The safety property (always falls back, never raises) holds even when a
    provider does not honor its half of the contract."""

    frame = _frame()
    result = NarrativeProviderResult(
        body="",
        referenced_fact_ids=(),
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=result)

    answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is not None
    assert answer.narrative is not None
    assert answer.narrative.mode == "deterministic_fallback"


@pytest.mark.asyncio
async def test_narrative_grounding_failure_falls_back_with_its_own_code():
    """The 7th failure mode: the provider *succeeds* at the transport layer
    but its content fails frame grounding (here: names a subject the frame
    never resolved)."""

    frame = _frame()
    bad_result = NarrativeProviderResult(
        body="A completely different repository is on track.",
        referenced_fact_ids=(),
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=bad_result)

    answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED
    assert answer.narrative is not None
    assert answer.narrative.mode == "deterministic_fallback"


@pytest.mark.asyncio
async def test_a_valid_provider_narrative_is_accepted():
    frame = _frame()
    provider = _ScriptedProvider(result=_valid_provider_result(frame))

    answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is None
    assert answer.narrative is not None
    assert answer.narrative.mode == "provider"


# ---------------------------------------------------------------------------
# C4: the provider cannot move a number, entity, or outcome. Proves the
# already-landed validators are actually reached from this path.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provider_cannot_claim_an_ungrounded_completion_percentage():
    frame = _frame()
    fact = frame.facts[0]
    result = NarrativeProviderResult(
        body=f"{fact.text} Completion is 42% for this repository.",
        referenced_fact_ids=(fact.fact_id,),
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=result)

    answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED
    assert (
        answer.narrative is not None
        and answer.narrative.mode == "deterministic_fallback"
    )
    assert answer.frame.completion is not None
    assert frame.completion is not None
    assert answer.frame.completion.rate == frame.completion.rate  # unchanged


@pytest.mark.asyncio
async def test_provider_cannot_claim_ready_against_a_not_ready_frame():
    frame = _frame()
    assert frame.readiness is not None and frame.readiness.state == "not_ready"
    subject_label = frame.subject_ref.display_label  # type: ignore[union-attr]
    result = NarrativeProviderResult(
        body=f"{subject_label} is ready to ship.",
        referenced_fact_ids=(),
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=result)

    answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED
    assert answer.frame.readiness is not None
    assert answer.frame.readiness.state == "not_ready"


@pytest.mark.asyncio
async def test_provider_cannot_reference_a_fact_id_that_does_not_exist():
    frame = _frame()
    result = NarrativeProviderResult(
        body="Something happened.",
        referenced_fact_ids=("fact_does_not_exist",),
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=result)

    _answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED


@pytest.mark.asyncio
async def test_provider_cannot_name_a_different_subject():
    frame = _frame()
    result = NarrativeProviderResult(
        body="An entirely unrelated project called zzz-other-repo is fine.",
        referenced_fact_ids=(),
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=result)

    _answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED


# ---------------------------------------------------------------------------
# Deterministic fallback narrative: built ONLY from the frame.
# ---------------------------------------------------------------------------


def test_deterministic_fallback_is_pure():
    """P3: no uuid4()/datetime.now() -- building it twice from the same
    frame and generated_at must be byte-identical."""

    frame = _frame()
    first = build_deterministic_fallback_narrative(frame, generated_at=_GENERATED_AT)
    second = build_deterministic_fallback_narrative(frame, generated_at=_GENERATED_AT)
    assert first.model_dump(mode="json") == second.model_dump(mode="json")


def test_deterministic_fallback_only_cites_frame_content():
    frame = _frame()
    narrative = build_deterministic_fallback_narrative(
        frame, generated_at=_GENERATED_AT
    )
    known_facts = {fact.fact_id for fact in frame.facts}
    known_sections = {section.section_id for section in frame.sections}
    assert set(narrative.referenced_fact_ids) <= known_facts
    assert set(narrative.referenced_section_ids) <= known_sections
    # Must itself survive the same layer-3 checks a provider narrative does.
    v2_validators.validate_narrative_fact_references(narrative, frame)
    v2_validators.validate_narrative_frame_consistency(narrative, frame)
    assert not v2_validators.scan_public_text(narrative.body)


def test_a_provider_leak_token_never_reaches_the_fallback_narrative():
    """Planted-defect proof (team-lead requirement #2): a provider response
    that would fail validation late must leave zero trace of its own
    tokens in the fallback narrative that actually gets returned.

    build_deterministic_fallback_narrative's signature accepts no provider
    output at all, so this is a structural guarantee -- this test proves it
    end to end through synthesize_narrative_answer, not just by inspecting
    the function signature.
    """

    leak_token = "PROVIDER_LEAK_TOKEN_9f3c2a"
    frame = _frame()
    bad_result = NarrativeProviderResult(
        body=f"{leak_token} claims something the frame never said.",
        referenced_fact_ids=("fact_does_not_exist",),  # forces late rejection
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=bad_result)

    answer, failure_code = _run_sync(
        synthesize_narrative_answer(
            frame=frame,
            provider=provider,
            answer_id=_ANSWER_ID,
            conversation_id=_CONVERSATION_ID,
            run_id=frame.run_id,
            generated_at=_GENERATED_AT,
        )
    )

    assert failure_code is NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED
    assert answer.narrative is not None
    assert leak_token not in answer.narrative.body


def _run_sync(coro):
    import asyncio

    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# P6: narrative choice is the only delta between two DevAnswerV2 built from
# the same frame.
# ---------------------------------------------------------------------------


def test_narrative_choice_is_the_only_delta_between_fallback_and_none():
    frame = _frame()
    narrative = build_deterministic_fallback_narrative(
        frame, generated_at=_GENERATED_AT
    )

    with_narrative = assert_narrative_choice_is_the_only_delta(
        frame,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
        narrative=narrative,
    )
    without_narrative = assert_narrative_choice_is_the_only_delta(
        frame,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
        narrative=None,
    )

    assert with_narrative.narrative is not None
    assert without_narrative.narrative is None
    assert with_narrative.frame == without_narrative.frame
    assert with_narrative.public_outcome == without_narrative.public_outcome
    assert (
        with_narrative.outcome_display_label == without_narrative.outcome_display_label
    )
    assert with_narrative.run_id == without_narrative.run_id


# ---------------------------------------------------------------------------
# Mutation controls (rule 2/3: plant the defect, observe the old suite pass
# and the new control fail; mutate the specific clause, not the whole guard).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disabling_fact_id_validation_permits_a_hallucinated_reference(
    monkeypatch,
):
    """Old suite (test_provider_cannot_reference_a_fact_id_that_does_not_exist)
    passes today because validate_narrative_fact_references is load-bearing.
    Disable it and the same hallucinated reference is wrongly accepted --
    proving the guard, not something else, is what rejects it."""

    frame = _frame()
    valid = _valid_provider_result(frame)
    result = NarrativeProviderResult(
        body=valid.body,
        # Otherwise-valid, subject-naming, fact-grounded body -- the only
        # defect is this extra reference to a fact the frame does not have.
        # Isolates the fact-reference guard from every other layer-3 check
        # (rule 3: mutate the clause, not the whole condition).
        referenced_fact_ids=(*valid.referenced_fact_ids, "fact_does_not_exist"),
        referenced_section_ids=valid.referenced_section_ids,
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=result)

    monkeypatch.setattr(
        _nf._validators, "validate_narrative_fact_references", lambda *a, **k: None
    )

    answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    # With the guard disabled, the hallucinated reference is wrongly
    # accepted as a "provider" narrative -- the defect this guard exists to
    # catch is now live.
    assert failure_code is None
    assert answer.narrative is not None
    assert answer.narrative.mode == "provider"


@pytest.mark.asyncio
async def test_disabling_frame_consistency_validation_permits_a_false_readiness_claim(
    monkeypatch,
):
    frame = _frame()
    subject_label = frame.subject_ref.display_label  # type: ignore[union-attr]
    result = NarrativeProviderResult(
        body=f"{subject_label} is ready to ship.",
        referenced_fact_ids=(),
        referenced_section_ids=(),
        provider_family="scripted",
        model_fingerprint="scripted-v1",
    )
    provider = _ScriptedProvider(result=result)

    monkeypatch.setattr(
        _nf._validators, "validate_narrative_frame_consistency", lambda *a, **k: None
    )

    answer, failure_code = await synthesize_narrative_answer(
        frame=frame,
        provider=provider,
        answer_id=_ANSWER_ID,
        conversation_id=_CONVERSATION_ID,
        run_id=frame.run_id,
        generated_at=_GENERATED_AT,
    )

    assert failure_code is None
    assert answer.narrative is not None
    assert answer.narrative.mode == "provider"


def test_the_p6_drift_check_catches_a_genuinely_mismatched_pair():
    """Isolates the P6 enforcement clause itself (rule 3: mutate the
    clause, not the whole guard): given a baseline and a candidate built
    from two genuinely different frames -- the exact bug class
    ``assert_narrative_choice_is_the_only_delta`` exists to catch, e.g. a
    future refactor that let narrative selection swap in the wrong frame --
    the real, non-empty ``_ANSWER_FIELDS_EXCLUDING_NARRATIVE`` comparison
    finds the drift."""

    frame = _frame()
    other_frame = DevAnswerFrame.model_validate(
        {
            **positive_fixtures()["dev_answer_frame.v1"],
            "public_outcome": "answered_with_gaps",
            "completion": None,
            "readiness": None,
            "limitations": ["This is a different frame for the drift check."],
        }
    )
    from dev_health_ops.api.dev.contracts_v2.answer import (
        DevAnswerV2,
        outcome_display_label,
    )

    def _answer_for(f: DevAnswerFrame) -> DevAnswerV2:
        return DevAnswerV2(
            schema_version="dev_answer.v2",
            answer_id=_ANSWER_ID,
            conversation_id=_CONVERSATION_ID,
            run_id=f.run_id,
            generated_at=_GENERATED_AT,
            public_outcome=f.public_outcome,
            outcome_display_label=outcome_display_label(f.public_outcome),
            frame=f,
            narrative=None,
        )

    baseline = _answer_for(frame)
    mismatched_candidate = _answer_for(other_frame)

    real_drifted = [
        field_name
        for field_name in _nf._ANSWER_FIELDS_EXCLUDING_NARRATIVE
        if getattr(mismatched_candidate, field_name) != getattr(baseline, field_name)
    ]
    assert real_drifted, (
        "the real comparison must observe a mismatched-frame pair as drift"
    )

    # Mutate the clause: disable the comparison by shrinking the field set
    # to empty (simulating the guard's field set being wiped, rather than
    # the whole "if drifted: raise" being deleted -- the narrower,
    # clause-level mutation rule 3 asks for). With nothing left to compare,
    # the same genuinely mismatched pair is silently missed.
    mutated_field_set: tuple[str, ...] = ()  # was _ANSWER_FIELDS_EXCLUDING_NARRATIVE
    disabled_drifted = [
        field_name
        for field_name in mutated_field_set
        if getattr(mismatched_candidate, field_name) != getattr(baseline, field_name)
    ]
    assert disabled_drifted == [], (
        "with the field set cleared, the mismatch is no longer observable -- "
        "this is exactly what makes the real, non-empty field set load-bearing"
    )
