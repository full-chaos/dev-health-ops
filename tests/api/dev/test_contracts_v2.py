"""Tests for the Ask Dev Wave 3.1 v2 contracts (CHAOS-3294).

Structure mirrors ``tests/api/dev/test_contracts.py`` (v1): generic
per-schema positive/negative parametrized checks, plus targeted tests for
each of the five acceptance-criteria semantic validators. Each of those
five gets a **fail-before/pass-after mutation pair**: the negative fixture
is shown to fail with all validators active, then the *one* validator that
rejects it is disabled via ``monkeypatch`` (patching the ``validators``
module object, not a bound method — see
``dev_health_ops.api.dev.contracts_v2.validators`` module docstring for why
that's what makes the guard independently disableable), and the same
payload is shown to now pass, while every *other* validator's fixture is
shown to still fail. That is the "removing each key semantic validator
fails the contract suite" acceptance clause, made attributable per-guard.
"""

from __future__ import annotations

import itertools
import re
from copy import deepcopy
from enum import StrEnum
from typing import Any, get_args, get_origin

import pytest
from pydantic import BaseModel, ValidationError

from dev_health_ops.api.dev import contracts_v2 as v2
from dev_health_ops.api.dev.contract_fixtures import (
    positive_fixtures as positive_fixtures_v1,
)
from dev_health_ops.api.dev.contract_fixtures_v2 import (
    NOW,
    _needs_clarification_frame_base,
    needs_clarification_frame_with_candidates,
    negative_fixtures,
    no_answer_answer_fixture,
    positive_fixtures,
    stream_fixtures,
)
from dev_health_ops.api.dev.contracts import (
    CONTRACT_MODELS,
    AnswerStatus,
    DevClaimFlags,
    DevContractVersions,
    DevCoverage,
    DevDisambiguationCandidate,
    DevEntityRef,
    DevEvidenceRef,
    DevModelMetadata,
    DevScope,
    DevScopeResolution,
    DevTimeRange,
    EntityType,
    ScopeResolutionOutcome,
)
from dev_health_ops.api.dev.contracts import DevAnswer as DevAnswerV1
from dev_health_ops.api.dev.contracts import DevError as DevErrorV1
from dev_health_ops.api.dev.contracts_v2 import compat as compat_module
from dev_health_ops.api.dev.contracts_v2 import validators as validators_module
from dev_health_ops.api.dev.contracts_v2.validators import ANSWERED_CONTENT_OUTCOMES
from dev_health_ops.api.dev.export_contracts_v2 import (
    check_artifacts,
    expected_artifacts,
)

# ---------------------------------------------------------------------------
# Generic per-schema coverage (mirrors test_contracts.py)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("schema_version", v2.CONTRACT_MODELS_V2)
def test_positive_fixture_validates(schema_version: str) -> None:
    v2.CONTRACT_MODELS_V2[schema_version].model_validate(
        positive_fixtures()[schema_version]
    )


@pytest.mark.parametrize(
    ("schema_version", "case", "payload"),
    [
        (schema_version, case, payload)
        for schema_version, cases in negative_fixtures().items()
        for case, payload in cases
    ],
)
def test_negative_fixture_is_rejected(
    schema_version: str, case: str, payload: dict[str, object]
) -> None:
    with pytest.raises(ValidationError, match=".+"):
        v2.CONTRACT_MODELS_V2[schema_version].model_validate(payload)


@pytest.mark.parametrize("schema_version", v2.CONTRACT_MODELS_V2)
def test_every_contract_requires_its_explicit_version(schema_version: str) -> None:
    payload = deepcopy(positive_fixtures()[schema_version])
    payload.pop("schema_version")
    with pytest.raises(ValidationError, match="schema_version"):
        v2.CONTRACT_MODELS_V2[schema_version].model_validate(payload)


def test_stream_sequences_require_exactly_one_terminal_then_done() -> None:
    fixtures = stream_fixtures()
    v2.validate_stream_v2(
        [v2.DevStreamEventV2.model_validate(item) for item in fixtures["valid"]]
    )
    for name, payloads in fixtures.items():
        if name == "valid":
            continue
        with pytest.raises((ValidationError, ValueError)):
            v2.validate_stream_v2(
                [v2.DevStreamEventV2.model_validate(item) for item in payloads]
            )


def test_checked_in_contract_artifacts_have_no_drift() -> None:
    check_artifacts(expected_artifacts())


def test_contract_schemas_are_provider_neutral_and_closed() -> None:
    artifacts = expected_artifacts()
    schemas = "\n".join(
        contents for path, contents in artifacts.items() if path.startswith("schemas/")
    )
    assert 'additionalProperties": false' in schemas
    for provider_specific in ("openai_api_key", "anthropic_api_key", "tool_choice"):
        assert provider_specific not in schemas


def test_v1_transcripts_remain_readable_alongside_v2() -> None:
    """Existing retained dev_answer.v1 transcripts remain readable (CHAOS-3294)."""

    v1_positives = positive_fixtures_v1()
    for schema_version, payload in v1_positives.items():
        CONTRACT_MODELS[schema_version].model_validate(payload)


# ---------------------------------------------------------------------------
# Request amendment: client question_class is a non-authoritative hint
# ---------------------------------------------------------------------------


def test_intent_requires_deprecation_warning_when_client_hint_present() -> None:
    payload = deepcopy(positive_fixtures()["dev_question_intent.v1"])
    payload["client_question_class_hint"] = "investigation"
    with pytest.raises(ValidationError, match="deprecation warning"):
        v2.DevQuestionIntent.model_validate(payload)

    payload["client_hint_deprecation_warning"] = (
        "question_class is client-supplied and non-authoritative; ignored for planning."
    )
    intent = v2.DevQuestionIntent.model_validate(payload)
    assert intent.client_question_class_hint == "investigation"


def test_message_request_v2_has_no_authoritative_question_class_field() -> None:
    schema = v2.DevMessageRequestV2.model_json_schema()
    assert "question_class" not in schema["properties"]
    assert "question_class_hint" in schema["properties"]


# ---------------------------------------------------------------------------
# TEAM as a first-class subject kind (v2 contract layer only)
# ---------------------------------------------------------------------------


def _team_entity_ref() -> dict[str, object]:
    return {
        "entity_kind": "team",
        "entity_id": "team_platform",
        "display_label": "Platform team",
        "repository_id": None,
        "team_id": "team_platform",
    }


def test_team_is_a_first_class_subject_set_kind() -> None:
    payload = deepcopy(positive_fixtures()["dev_subject_set.v1"])
    payload["entity_kind"] = "team"
    payload["committed_entity_refs"] = [_team_entity_ref()]
    subject_set = v2.DevSubjectSet.model_validate(payload)
    assert subject_set.entity_kind is v2.EntityKind.TEAM


def test_subject_set_rejects_heterogeneous_entity_kinds() -> None:
    payload = deepcopy(positive_fixtures()["dev_subject_set.v1"])
    payload["committed_entity_refs"].append(_team_entity_ref())
    with pytest.raises(ValidationError, match="homogeneous"):
        v2.DevSubjectSet.model_validate(payload)


# ---------------------------------------------------------------------------
# dev_resolution_ledger.v1 append-only guarantee (cross-snapshot)
# ---------------------------------------------------------------------------


def _second_entry() -> dict[str, object]:
    return {
        "entry_ordinal": 1,
        "mention_id": "0f1a2b3c-0009-4a00-8000-000000000002",
        "outcome": "no_authorized_match",
        "committed_entity_ref": None,
        "candidates": [],
        "repository_attribution": None,
        "team_attribution": None,
        "resolver_version": "resolver.v1",
        "query_version": "resolve_scope.v1",
        "resolved_at": NOW,
    }


def test_ledger_extension_accepts_pure_append() -> None:
    previous = v2.DevResolutionLedger.model_validate(
        positive_fixtures()["dev_resolution_ledger.v1"]
    )
    extended_payload = deepcopy(positive_fixtures()["dev_resolution_ledger.v1"])
    extended_payload["mention_ids"].append("0f1a2b3c-0009-4a00-8000-000000000002")
    extended_payload["entries"].append(_second_entry())
    extended = v2.DevResolutionLedger.model_validate(extended_payload)

    v2.validate_ledger_extends(previous, extended)  # must not raise


def test_ledger_extension_rejects_rewriting_a_prior_entry() -> None:
    previous = v2.DevResolutionLedger.model_validate(
        positive_fixtures()["dev_resolution_ledger.v1"]
    )
    extended_payload = deepcopy(positive_fixtures()["dev_resolution_ledger.v1"])
    extended_payload["mention_ids"].append("0f1a2b3c-0009-4a00-8000-000000000002")
    extended_payload["entries"].append(_second_entry())

    rewritten_payload = deepcopy(extended_payload)
    rewritten_payload["entries"][0]["outcome"] = "no_authorized_match"
    rewritten_payload["entries"][0]["committed_entity_ref"] = None
    rewritten = v2.DevResolutionLedger.model_validate(rewritten_payload)

    with pytest.raises(ValueError, match="cannot rewrite or erase"):
        v2.validate_ledger_extends(previous, rewritten)


def test_ledger_extension_rejects_shrinking() -> None:
    previous = v2.DevResolutionLedger.model_validate(
        positive_fixtures()["dev_resolution_ledger.v1"]
    )
    extended_payload = deepcopy(positive_fixtures()["dev_resolution_ledger.v1"])
    extended_payload["mention_ids"].append("0f1a2b3c-0009-4a00-8000-000000000002")
    extended_payload["entries"].append(_second_entry())
    extended = v2.DevResolutionLedger.model_validate(extended_payload)

    with pytest.raises(ValueError, match="cannot shrink"):
        v2.validate_ledger_extends(extended, previous)


# ---------------------------------------------------------------------------
# The five acceptance-criteria semantic validators, isolated one at a time
# ---------------------------------------------------------------------------

_FRAME_VALIDATOR_CASES: dict[str, tuple[str, str]] = {
    "validate_no_internal_leakage": ("dev_answer_frame.v1", "internal_leakage"),
    "validate_outcome_consistency": ("dev_answer_frame.v1", "outcome_content_mismatch"),
    "validate_completion_denominator": (
        "dev_answer_frame.v1",
        "completion_without_denominator",
    ),
    "validate_relationship_refs_within_frame": (
        "dev_answer_frame.v1",
        "relationship_outside_frame",
    ),
    # Round 3: making ``versions`` optional (so a no-answer outcome can omit
    # it) must not have made it droppable from a frame that carries content.
    "validate_versions_presence": (
        "dev_answer_frame.v1",
        "answered_without_versions",
    ),
}

#: A validator can reject more than one negative fixture. CHAOS-3297 flags
#: gap: ``answered_with_disclosure`` is a second case
#: ``validate_outcome_consistency`` alone rejects (its new fact-disclosure
#: clause), alongside ``outcome_content_mismatch`` (the pre-existing
#: has_content clause) -- both must flip when the validator is disabled, and
#: neither should count as "a different guardrail" in the isolation loop below.
_ADDITIONAL_CASES_SAME_VALIDATOR: dict[str, tuple[str, ...]] = {
    # CHAOS-3325: answered_with_clarification_candidates is a second case
    # validate_outcome_consistency alone rejects (its new clarification-
    # candidates clause), alongside answered_with_disclosure (CHAOS-3297) and
    # outcome_content_mismatch (the pre-existing has_content clause).
    "validate_outcome_consistency": (
        "answered_with_disclosure",
        "answered_with_clarification_candidates",
    ),
}


@pytest.mark.parametrize("validator_name", sorted(_FRAME_VALIDATOR_CASES))
def test_disabling_one_frame_validator_flips_only_its_own_fixture(
    monkeypatch: pytest.MonkeyPatch, validator_name: str
) -> None:
    schema_version, target_case = _FRAME_VALIDATOR_CASES[validator_name]
    all_cases = negative_fixtures()[schema_version]
    target_payload = dict(all_cases)[target_case]
    same_validator_cases = {
        target_case,
        *_ADDITIONAL_CASES_SAME_VALIDATOR.get(validator_name, ()),
    }

    # Baseline: rejected with every validator active.
    with pytest.raises(ValidationError):
        v2.DevAnswerFrame.model_validate(target_payload)

    monkeypatch.setattr(validators_module, validator_name, lambda *_a, **_k: None)

    # Every fixture attributable to this validator now passes: the disabled
    # guard was the only thing rejecting each of them.
    for same_case in same_validator_cases:
        v2.DevAnswerFrame.model_validate(dict(all_cases)[same_case])

    # Every other case for this schema is a different guardrail and must
    # still be rejected — proves the mutation is attributable to exactly this
    # validator's cases, not a global bypass.
    for other_case, other_payload in all_cases:
        if other_case in same_validator_cases:
            continue
        with pytest.raises(ValidationError):
            v2.DevAnswerFrame.model_validate(other_payload)


def test_all_five_named_validators_are_distinct_functions() -> None:
    """Sanity check the five acceptance-criteria guardrails are not aliases
    of one another (a bug that would make the isolation tests above vacuous)."""

    names = [
        "validate_no_internal_leakage",
        "validate_outcome_consistency",
        "validate_completion_denominator",
        "validate_narrative_fact_references",
        "validate_relationship_refs_within_frame",
    ]
    functions = [getattr(validators_module, name) for name in names]
    assert len(functions) == len(set(functions))


# ---------------------------------------------------------------------------
# (d) narrative referencing missing fact IDs — cross-object validator
# ---------------------------------------------------------------------------


def test_narrative_missing_fact_reference_is_rejected() -> None:
    answer_payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    answer_payload["narrative"]["referenced_fact_ids"] = ["fact_unknown"]
    with pytest.raises(ValidationError, match="unknown fact"):
        v2.DevAnswerV2.model_validate(answer_payload)


def test_disabling_narrative_validator_flips_only_its_own_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    answer_payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    answer_payload["narrative"]["referenced_fact_ids"] = ["fact_unknown"]
    with pytest.raises(ValidationError):
        v2.DevAnswerV2.model_validate(answer_payload)

    monkeypatch.setattr(
        validators_module, "validate_narrative_fact_references", lambda *_a, **_k: None
    )
    v2.DevAnswerV2.model_validate(answer_payload)  # now passes

    # A different dev_answer.v2 guardrail (outcome/frame mismatch) must still
    # reject — this validator's removal did not disable answer-level checks.
    for _case, payload in negative_fixtures()["dev_answer.v2"]:
        with pytest.raises(ValidationError):
            v2.DevAnswerV2.model_validate(payload)


def test_narrative_section_reference_must_exist_in_frame() -> None:
    answer_payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    answer_payload["narrative"]["referenced_section_ids"] = ["section_unknown"]
    with pytest.raises(ValidationError, match="unknown section"):
        v2.DevAnswerV2.model_validate(answer_payload)


# ---------------------------------------------------------------------------
# CHAOS-3297 flags gap ("DevAnswerFact flags gap — design ratified
# 2026-08-02"): DevAnswerFact.disclosures, its canonical-order validator, the
# answered/disclosure clause on validate_outcome_consistency, and the
# exhaustive v2-to-v1 half of the round-trip oracle. The v1-to-v2 half
# (wrap_legacy_answer_as_frame) lives in test_terminal_frames.py.
# ---------------------------------------------------------------------------


def test_disclosures_out_of_order_is_rejected() -> None:
    payload = dict(negative_fixtures()["dev_answer_frame.v1"])[
        "disclosures_out_of_order"
    ]
    with pytest.raises(ValidationError, match="ascending"):
        v2.DevAnswerFrame.model_validate(payload)


def test_disclosures_duplicated_is_rejected() -> None:
    payload = dict(negative_fixtures()["dev_answer_frame.v1"])["disclosures_duplicated"]
    with pytest.raises(ValidationError, match="ascending"):
        v2.DevAnswerFrame.model_validate(payload)


def test_disclosures_canonical_order_accepts_the_full_ascending_tuple() -> None:
    payload = deepcopy(positive_fixtures()["dev_answer_frame.v1"])
    payload["facts"][0]["disclosures"] = [member.value for member in v2.FactDisclosure]
    payload["public_outcome"] = "answered_with_gaps"
    payload["limitations"] = ["Some facts on this answer carry disclosures."]
    payload["completion"] = None
    payload["readiness"] = None
    frame = v2.DevAnswerFrame.model_validate(payload)
    assert frame.facts[0].disclosures == tuple(v2.FactDisclosure)


def test_answered_disclosure_clause_is_new_not_preexisting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RED/GREEN pair for the CHAOS-3297 flags-gap 'answered' clause.

    An 'answered' frame whose only defect is one fact disclosure passed
    ``validate_outcome_consistency`` before this changeset (the function had
    no knowledge of ``DevAnswerFact.disclosures`` at all) and is rejected
    after it. The "old" body below is byte-identical to
    ``validators.validate_outcome_consistency`` minus the new disclosure
    clause -- copied rather than imported from git history so the RED half
    is self-contained and does not depend on checking out a prior commit.
    """

    payload = dict(negative_fixtures()["dev_answer_frame.v1"])[
        "answered_with_disclosure"
    ]
    original_validator = validators_module.validate_outcome_consistency

    def _pre_flags_gap_validate_outcome_consistency(frame: Any) -> None:
        outcome = frame.public_outcome.value
        has_content = bool(frame.sections) or bool(frame.facts)
        if outcome in validators_module._EMPTY_CONTENT_OUTCOMES and has_content:
            raise ValueError(
                f"public outcome {outcome!r} cannot carry answer sections/facts"
            )
        if outcome in validators_module.ANSWERED_CONTENT_OUTCOMES and not has_content:
            raise ValueError(
                f"public outcome {outcome!r} requires answer sections and facts"
            )
        if outcome == "answered":
            if frame.limitations:
                raise ValueError(
                    "'answered' cannot carry limitations; use answered_with_gaps"
                )
            if frame.completion is not None and frame.completion.calculable is False:
                raise ValueError(
                    "'answered' cannot carry a non-calculable completion block; "
                    "use answered_with_gaps"
                )
        if outcome == "answered_with_gaps" and not frame.limitations:
            if frame.completion is None or frame.completion.calculable is not False:
                raise ValueError(
                    "'answered_with_gaps' requires disclosed limitations or a "
                    "non-calculable completion block"
                )

    # RED: the pre-changeset validator body has no disclosure clause and
    # accepts this fixture.
    monkeypatch.setattr(
        validators_module,
        "validate_outcome_consistency",
        _pre_flags_gap_validate_outcome_consistency,
    )
    v2.DevAnswerFrame.model_validate(payload)  # must not raise

    # Every other frame negative fixture is a different guardrail (internal
    # leakage, outcome/content mismatch, completion denominator, ...) and
    # must still be rejected under the RED replacement -- proving it is a
    # faithful full reproduction of the pre-changeset validator, not a stub
    # that happens to also let the disclosure fixture through by disabling
    # everything. CHAOS-3325's clarification-candidates clause is excluded
    # too: this reproduction is frozen at the pre-3297 snapshot, which
    # predates that clause as well, so it correctly passes that fixture for
    # the same reason it passes the disclosure one -- not a different guard
    # catching it.
    for other_case, other_payload in negative_fixtures()["dev_answer_frame.v1"]:
        if other_case in (
            "answered_with_disclosure",
            "answered_with_clarification_candidates",
        ):
            continue
        with pytest.raises(ValidationError):
            v2.DevAnswerFrame.model_validate(other_payload)

    # GREEN: restore the real, current validator -- it rejects the same payload.
    monkeypatch.setattr(
        validators_module, "validate_outcome_consistency", original_validator
    )
    with pytest.raises(ValidationError, match="disclosure"):
        v2.DevAnswerFrame.model_validate(payload)


def _all_disclosure_subsets() -> list[tuple[v2.FactDisclosure, ...]]:
    """Every one of the 2**4 canonically-ordered disclosure subsets.

    ``itertools.combinations`` over an already enum-ordered sequence
    preserves that order, so every subset it yields is already the
    canonical form ``DevAnswerFact`` requires.
    """

    members = list(v2.FactDisclosure)
    return [
        combo
        for size in range(len(members) + 1)
        for combo in itertools.combinations(members, size)
    ]


@pytest.mark.parametrize(
    "subset",
    _all_disclosure_subsets(),
    ids=lambda s: "+".join(d.value for d in s) or "empty",
)
def test_compat_projects_fact_disclosures_to_claim_flags_exhaustively(
    subset: tuple[Any, ...],
) -> None:
    """Exhaustive 2**4 oracle, v2-to-v1 half: every disclosure subset on a
    frame's one fact round-trips to exactly the matching v1 ``DevClaimFlags``
    bits, with everything else False. The v1-to-v2 half
    (``wrap_legacy_answer_as_frame``) is
    ``test_wrap_legacy_answer_round_trips_claim_flags_exhaustively`` in
    test_terminal_frames.py.
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["frame"]["facts"][0]["disclosures"] = [d.value for d in subset]
    if subset:
        # A non-empty disclosure set is forbidden on 'answered' (the new
        # clause under test above); use 'answered_with_gaps' with a
        # disclosed limitation for every non-empty subset so this test
        # exercises the projector, not the outcome-consistency guard.
        payload["public_outcome"] = "answered_with_gaps"
        payload["outcome_display_label"] = "Answered with some gaps"
        payload["frame"]["public_outcome"] = "answered_with_gaps"
        payload["frame"]["limitations"] = [
            "One or more facts on this answer carry a disclosure."
        ]
    # The scripted narrative only narrates the empty-disclosure body; drop it
    # so this test stays focused on the frame/claim projection, not narrative
    # consistency (covered elsewhere).
    payload["narrative"] = None
    answer = v2.DevAnswerV2.model_validate(payload)

    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevAnswerV1)
    claim = projected.claims[0]
    expected_flags = DevClaimFlags(**{d.value: True for d in subset})
    assert claim.flags == expected_flags


# ---------------------------------------------------------------------------
# Measured-zero vs. no-data (deliverable-list guardrail on dev_source_observation.v1)
# ---------------------------------------------------------------------------


def test_source_observation_cannot_claim_measured_zero_when_unmeasured() -> None:
    payload = deepcopy(positive_fixtures()["dev_source_observation.v1"])
    payload["observed_state"] = "unconfigured"
    payload["limitation"] = "Provider not configured."
    with pytest.raises(ValidationError, match="cannot claim a measured zero"):
        v2.DevSourceObservation.model_validate(payload)


def test_source_observation_measured_zero_is_distinguishable_from_no_data() -> None:
    measured_zero = v2.DevSourceObservation.model_validate(
        positive_fixtures()["dev_source_observation.v1"]
    )
    assert measured_zero.usable_fact_count == 0
    assert measured_zero.data_semantics == "measured_zero"

    no_data_payload = deepcopy(positive_fixtures()["dev_source_observation.v1"])
    no_data_payload["observed_state"] = "unavailable"
    no_data_payload["data_semantics"] = "no_data"
    no_data_payload["limitation"] = "Source connection timed out."
    no_data = v2.DevSourceObservation.model_validate(no_data_payload)
    assert no_data.data_semantics == "no_data"
    assert measured_zero.data_semantics != no_data.data_semantics


# ---------------------------------------------------------------------------
# The v2-to-v1 compatibility projector
# ---------------------------------------------------------------------------


def no_answer_payload(outcome: str) -> dict[str, Any]:
    return deepcopy(no_answer_answer_fixture(outcome))


def _time_range_for_scope() -> DevTimeRange:
    return DevScope.model_validate(
        positive_fixtures()["dev_message_request.v2"]["scope"]
    ).time_range


def test_compat_projects_answered_to_complete_v1_answer() -> None:
    answer = v2.DevAnswerV2.model_validate(positive_fixtures()["dev_answer.v2"])
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevAnswerV1)
    assert projected.schema_version == "dev_answer.v1"
    assert projected.status is AnswerStatus.COMPLETE
    assert projected.direct_summary == answer.frame.direct_answer
    # v1 CONTRACT_MODELS accepts the projected object's own dict form too.
    CONTRACT_MODELS["dev_answer.v1"].model_validate(projected.model_dump(mode="json"))


def test_compat_downgrades_to_partial_when_coverage_is_incomplete() -> None:
    answer_payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    answer_payload["frame"]["coverage"] = {
        "required_source_count": 2,
        "available_source_count": 1,
        "unavailable_required_sources": [v2.SourceClass.DEPLOYMENT.value],
        "stale_required_sources": [],
        "as_of": NOW,
    }
    answer_payload["public_outcome"] = "answered_with_gaps"
    answer_payload["outcome_display_label"] = "Answered with some gaps"
    answer_payload["frame"]["public_outcome"] = "answered_with_gaps"
    answer_payload["frame"]["limitations"] = ["Deployment health was unavailable."]
    answer = v2.DevAnswerV2.model_validate(answer_payload)
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevAnswerV1)
    assert projected.status is AnswerStatus.PARTIAL


def test_compat_projects_needs_clarification_to_insufficient_evidence() -> None:
    answer_payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    answer_payload["frame"]["public_outcome"] = "needs_clarification"
    answer_payload["frame"]["sections"] = []
    answer_payload["frame"]["facts"] = []
    answer_payload["frame"]["completion"] = None
    answer_payload["frame"]["readiness"] = None
    answer_payload["frame"]["direct_answer"] = "Which repository did you mean?"
    answer_payload["public_outcome"] = "needs_clarification"
    answer_payload["outcome_display_label"] = "Needs clarification"
    answer_payload["narrative"] = None
    answer = v2.DevAnswerV2.model_validate(answer_payload)
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevAnswerV1)
    assert projected.schema_version == "dev_answer.v1"
    assert projected.status is AnswerStatus.INSUFFICIENT_EVIDENCE


# ---------------------------------------------------------------------------
# CHAOS-3325: a typed clarification-candidate block on dev_answer_frame.v1,
# carrying the resolution ledger's real authorized candidates instead of the
# v1 projector's pre-3325 fabricated placeholder.
# ---------------------------------------------------------------------------


def _needs_clarification_answer_payload(
    frame_payload: dict[str, Any],
) -> dict[str, Any]:
    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["frame"] = frame_payload
    payload["public_outcome"] = "needs_clarification"
    payload["outcome_display_label"] = "Needs clarification"
    payload["narrative"] = None
    return payload


def test_needs_clarification_frame_may_carry_zero_or_many_clarification_candidates() -> (
    None
):
    """Both shapes are legal on the contract: zero candidates (e.g. the
    question could not be interpreted at all) is not an error state that
    needs a placeholder to satisfy the type."""

    zero = v2.DevAnswerFrame.model_validate(_needs_clarification_frame_base())
    assert zero.clarification_candidates == ()

    many = v2.DevAnswerFrame.model_validate(needs_clarification_frame_with_candidates())
    assert [c.entity_ref.entity_id for c in many.clarification_candidates] == [
        "repo_nightfall_public",
        "repo_nightfall_internal",
    ]


#: Every outcome that may NOT carry clarification_candidates, derived from the
#: enum rather than listed: a ninth PublicOutcome member is parametrized into
#: the rejection test below the moment it is declared, and fails there until it
#: is classified into one of the two guards.
_CANDIDATE_GUARDED_OUTCOMES = tuple(
    sorted(
        outcome.value
        for outcome in v2.PublicOutcome
        if outcome is not v2.PublicOutcome.NEEDS_CLARIFICATION
    )
)


def _candidate_bearing_frame_for(outcome: str) -> dict[str, Any]:
    """One valid-but-for-the-candidates frame, retargeted at ``outcome``.

    Built by retargeting the two committed negative fixtures rather than
    hand-authoring a payload, so the candidate block under test is the exact
    one those fixtures pin (Verification rule: build from the real producer).
    Every other field is made legal for the outcome, so the ValidationError
    the test asserts is attributable to ``clarification_candidates`` and not
    to some unrelated invariant the retargeting broke.
    """

    negatives = dict(negative_fixtures()["dev_answer_frame.v1"])
    if outcome in v2.NO_ANSWER_OUTCOMES:
        payload = deepcopy(negatives["denied_with_clarification_candidates"])
        payload["public_outcome"] = outcome
        payload["direct_answer"] = v2.CANONICAL_NO_ANSWER_COPY[outcome]
        return payload
    payload = deepcopy(negatives["answered_with_clarification_candidates"])
    payload["public_outcome"] = outcome
    if outcome == "answered_with_gaps":
        # answered_with_gaps independently requires a disclosed gap, so give
        # it one -- otherwise the rejection could be that missing gap rather
        # than the candidates.
        payload["limitations"] = ["A required source was stale at query time."]
    return payload


def test_clarification_candidate_guards_partition_every_public_outcome() -> None:
    """The two guards partition ``PublicOutcome`` exactly, with no gap.

    Guard A is the ``ABSENT`` cell in ``NO_ANSWER_FRAME_FIELD_POLICY``, which
    only reaches ``NO_ANSWER_OUTCOMES``; guard B is
    ``validate_outcome_consistency``'s clause, which only reaches
    ``ANSWERED_CONTENT_OUTCOMES``. ``needs_clarification`` is in neither, by
    design -- it is the one outcome permitted to carry candidates. Asserting
    the three sets tile the enum is what makes "forbidden everywhere else"
    a closure rather than a claim about the cells someone happened to test.
    """

    guard_a = set(v2.NO_ANSWER_OUTCOMES)
    guard_b = set(ANSWERED_CONTENT_OUTCOMES)
    every_outcome = {outcome.value for outcome in v2.PublicOutcome}

    assert guard_a & guard_b == set(), "an outcome cannot be governed by both guards"
    assert guard_a | guard_b == every_outcome - {"needs_clarification"}
    assert set(_CANDIDATE_GUARDED_OUTCOMES) == guard_a | guard_b
    assert (
        v2.NO_ANSWER_FRAME_FIELD_POLICY["clarification_candidates"]
        is v2.NoAnswerFieldPolicy.ABSENT
    )


@pytest.mark.parametrize("outcome", _CANDIDATE_GUARDED_OUTCOMES)
def test_clarification_candidates_forbidden_outside_needs_clarification(
    outcome: str,
) -> None:
    """(f)/(b): every outcome but ``needs_clarification`` rejects candidates.

    Total over the enum, not a sample of two cells: the earlier version of
    this test exercised only ``denied`` and ``answered``, so a regression
    opening any of the other five would not have been observed.
    """

    payload = _candidate_bearing_frame_for(outcome)
    assert payload["clarification_candidates"], "the case must actually carry some"
    with pytest.raises(ValidationError, match="clarification_candidates"):
        v2.DevAnswerFrame.model_validate(payload)


def test_needs_clarification_is_the_one_outcome_that_accepts_candidates() -> None:
    """The positive half of the partition -- without it the test above is
    satisfied by a validator that rejects candidates unconditionally."""

    frame = v2.DevAnswerFrame.model_validate(
        needs_clarification_frame_with_candidates()
    )
    assert frame.public_outcome is v2.PublicOutcome.NEEDS_CLARIFICATION
    assert len(frame.clarification_candidates) == 2


def test_answered_clarification_candidates_clause_is_new_not_preexisting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RED/GREEN pair for the CHAOS-3325 'answered' clause on
    validate_outcome_consistency, mirroring
    test_answered_disclosure_clause_is_new_not_preexisting. The "old" body
    below is byte-identical to the pre-3325 validator (copied, not imported
    from history, so the RED half is self-contained)."""

    payload = dict(negative_fixtures()["dev_answer_frame.v1"])[
        "answered_with_clarification_candidates"
    ]

    def _pre_3325_validate_outcome_consistency(frame: Any) -> None:
        outcome = frame.public_outcome.value
        has_content = bool(frame.sections) or bool(frame.facts)
        if outcome in validators_module._EMPTY_CONTENT_OUTCOMES and has_content:
            raise ValueError(
                f"public outcome {outcome!r} cannot carry answer sections/facts"
            )
        if outcome in validators_module.ANSWERED_CONTENT_OUTCOMES and not has_content:
            raise ValueError(
                f"public outcome {outcome!r} requires answer sections and facts"
            )
        if outcome == "answered":
            if frame.limitations:
                raise ValueError(
                    "'answered' cannot carry limitations; use answered_with_gaps"
                )
            if frame.completion is not None and frame.completion.calculable is False:
                raise ValueError(
                    "'answered' cannot carry a non-calculable completion block; "
                    "use answered_with_gaps"
                )
            disclosed_facts = [fact.fact_id for fact in frame.facts if fact.disclosures]
            if disclosed_facts:
                raise ValueError(
                    "'answered' cannot carry a fact disclosure (stale/uncertain/"
                    f"conflicting/untrusted_source) on fact(s) {sorted(disclosed_facts)}; "
                    "use answered_with_gaps"
                )
        if outcome == "answered_with_gaps" and not frame.limitations:
            if frame.completion is None or frame.completion.calculable is not False:
                raise ValueError(
                    "'answered_with_gaps' requires disclosed limitations or a "
                    "non-calculable completion block"
                )

    original_validator = validators_module.validate_outcome_consistency

    # RED: the pre-changeset validator body has no clarification-candidates
    # clause and accepts this fixture.
    monkeypatch.setattr(
        validators_module,
        "validate_outcome_consistency",
        _pre_3325_validate_outcome_consistency,
    )
    v2.DevAnswerFrame.model_validate(payload)  # must not raise

    # Every other frame negative fixture is a different guardrail and must
    # still be rejected -- proves the RED body is a faithful reproduction,
    # not a stub that disables everything.
    for other_case, other_payload in negative_fixtures()["dev_answer_frame.v1"]:
        if other_case == "answered_with_clarification_candidates":
            continue
        with pytest.raises(ValidationError):
            v2.DevAnswerFrame.model_validate(other_payload)

    # GREEN: restore the real, current validator -- it rejects the same
    # payload.
    monkeypatch.setattr(
        validators_module, "validate_outcome_consistency", original_validator
    )
    with pytest.raises(ValidationError, match="clarification_candidates"):
        v2.DevAnswerFrame.model_validate(payload)


def test_clarification_candidate_entity_ids_must_be_unique() -> None:
    payload = dict(negative_fixtures()["dev_answer_frame.v1"])[
        "clarification_candidates_duplicate_entity_id"
    ]
    with pytest.raises(ValidationError, match="unique"):
        v2.DevAnswerFrame.model_validate(payload)


def test_clarification_candidate_entity_kind_is_a_closed_vocabulary() -> None:
    """The only "unauthorized-shaped" candidate expressible at the wire-type
    level: a candidate outside the closed EntityKind vocabulary. Real
    authorization is enforced by the builder (subject_preflight only ever
    constructs a candidate from an AuthorizedEntity the catalog itself
    returned), never by a field on the wire shape -- there is no
    "unauthorized" flag to smuggle a different value through."""

    payload = dict(negative_fixtures()["dev_answer_frame.v1"])[
        "clarification_candidate_unknown_entity_kind"
    ]
    with pytest.raises(ValidationError, match="entity_kind"):
        v2.DevAnswerFrame.model_validate(payload)


def test_compat_projects_real_clarification_candidates_not_a_placeholder() -> None:
    """GREEN: the current projector carries the frame's own authorized
    candidates -- the resolution ledger's real entries -- through to v1,
    reporting AMBIGUOUS (matching DevScopeResolution's own "ambiguous
    requires candidates" invariant)."""

    answer = v2.DevAnswerV2.model_validate(
        _needs_clarification_answer_payload(needs_clarification_frame_with_candidates())
    )
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevAnswerV1)
    resolution = projected.resolved_scope
    assert resolution.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert [c.entity_ref.entity_id for c in resolution.candidates] == [
        "repo_nightfall_public",
        "repo_nightfall_internal",
    ]
    assert [c.entity_ref.display_label for c in resolution.candidates] == [
        "full-chaos/nightfall-public",
        "full-chaos/nightfall-internal",
    ]
    assert all(c.reason for c in resolution.candidates)
    # v1 CONTRACT_MODELS accepts the projected object's own dict form too.
    CONTRACT_MODELS["dev_answer.v1"].model_validate(projected.model_dump(mode="json"))


def _pre_chaos_3325_project_needs_clarification(
    answer: v2.DevAnswerV2, organization_id: str, time_range: DevTimeRange
) -> DevAnswerV1:
    """Byte-identical to ``compat._project_needs_clarification`` before
    CHAOS-3325 -- copied, not imported from git history, so the RED half
    below is self-contained. Fabricates a placeholder candidate
    (``entity_id="clarification_required"``) whenever the frame carries
    neither a real candidate list nor a ``subject_ref``."""

    frame = answer.frame
    versions = compat_module._require_versions(frame.versions, answer.public_outcome)
    resolved = compat_module._build_resolved_scope(answer, organization_id, time_range)
    resolution = DevScopeResolution(
        schema_version="dev_scope_resolution.v1",
        requested_scope=resolved,
        resolved_scope=None,
        outcome=ScopeResolutionOutcome.AMBIGUOUS,
        authorized_repository_ids=[],
        authorized_entity_ids=[],
        candidates=[
            DevDisambiguationCandidate(
                entity_ref=DevEntityRef(
                    entity_type=compat_module._KIND_TO_ENTITY_TYPE.get(
                        frame.subject_ref.entity_kind, EntityType.REPOSITORY
                    ),
                    entity_id=frame.subject_ref.entity_id,
                    display_label=frame.subject_ref.display_label,
                    repository_id=frame.subject_ref.repository_id,
                ),
                reason="Clarification requested before continuing.",
            )
        ]
        if frame.subject_ref is not None
        else [
            DevDisambiguationCandidate(
                entity_ref=DevEntityRef(
                    entity_type=EntityType.REPOSITORY,
                    entity_id="clarification_required",
                    display_label="Clarification required",
                ),
                reason="The question requires clarification before it can be answered.",
            )
        ],
        fallbacks=[],
        warnings=[],
        resolved_at=answer.generated_at,
    )
    return DevAnswerV1(
        schema_version="dev_answer.v1",
        answer_id=answer.answer_id,
        conversation_id=answer.conversation_id,
        generated_at=answer.generated_at,
        resolved_scope=resolution,
        as_of=answer.generated_at,
        status=AnswerStatus.INSUFFICIENT_EVIDENCE,
        direct_summary=frame.direct_answer,
        claims=[],
        metrics=[],
        evidence=[],
        conflicts=[],
        coverage=compat_module._as_v1(DevCoverage, frame.coverage),
        warnings=list(frame.limitations),
        suggested_follow_up_questions=list(frame.safe_follow_up_questions),
        versions=DevContractVersions(
            prompt_version=versions.prompt_version
            or compat_module._DETERMINISTIC_VERSION_PLACEHOLDER,
            tool_contract_version=versions.tool_contract_version,
            metric_definition_version=versions.metric_definition_version,
            query_version=versions.query_version,
        ),
        model=DevModelMetadata(
            provider_source="platform",
            provider_family="deterministic",
            model_fingerprint=compat_module._DETERMINISTIC_VERSION_PLACEHOLDER,
        ),
    )


def test_needs_clarification_zero_candidates_no_longer_fabricates_a_placeholder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RED/GREEN pair: the pre-CHAOS-3325 projector invented a
    ``clarification_required`` placeholder candidate for the zero-candidate,
    no-subject_ref case (e.g. the question could not be interpreted at all).
    The current projector reports UNRESOLVED with no candidates instead --
    honest about "nothing to offer" rather than fabricating an option."""

    answer = v2.DevAnswerV2.model_validate(
        _needs_clarification_answer_payload(_needs_clarification_frame_base())
    )
    time_range = _time_range_for_scope()
    original = compat_module._project_needs_clarification

    # RED: the old projector fabricates a placeholder candidate and reports
    # AMBIGUOUS, even though nothing was ever actually ambiguous.
    monkeypatch.setattr(
        compat_module,
        "_project_needs_clarification",
        _pre_chaos_3325_project_needs_clarification,
    )
    fabricated = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=time_range
    )
    assert isinstance(fabricated, DevAnswerV1)
    assert fabricated.resolved_scope.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert len(fabricated.resolved_scope.candidates) == 1
    assert (
        fabricated.resolved_scope.candidates[0].entity_ref.entity_id
        == "clarification_required"
    )

    # GREEN: restore the real projector -- no fabrication.
    monkeypatch.setattr(compat_module, "_project_needs_clarification", original)
    honest = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=time_range
    )
    assert isinstance(honest, DevAnswerV1)
    assert honest.resolved_scope.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert honest.resolved_scope.candidates == []


@pytest.mark.parametrize(
    ("outcome", "expected_code"),
    [
        ("not_found", "scope_not_found"),
        ("temporarily_unavailable", "source_unavailable"),
        ("unsupported", "feature_not_enabled"),
        ("denied", "forbidden"),
        ("failed", "internal_error"),
    ],
)
def test_compat_projects_empty_content_outcomes_to_v1_error(
    outcome: str, expected_code: str
) -> None:
    answer = v2.DevAnswerV2.model_validate(no_answer_payload(outcome))
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevErrorV1)
    assert projected.code == expected_code
    # The v1 boundary emits the same server-owned copy the frame is pinned
    # to, built from the table rather than carried across from the frame.
    assert projected.safe_message == v2.CANONICAL_NO_ANSWER_COPY[outcome]


def test_compat_never_mislabels_a_team_subject_answer() -> None:
    answer_payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    answer_payload["frame"]["subject_ref"] = _team_entity_ref()
    answer_payload["frame"]["relationship_paths"] = [
        {
            **positive_fixtures()["dev_answer_frame.v1"]["relationship_paths"][0],
            "source_entity_id": "team_platform",
        }
    ]
    # Narrative subject-claim consistency is a separate guardrail under test
    # elsewhere; drop it here so this test stays focused on the compat
    # projector's team-scope handling.
    answer_payload["narrative"] = None
    answer = v2.DevAnswerV2.model_validate(answer_payload)
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevErrorV1)
    assert projected.code == "feature_not_enabled"


# ---------------------------------------------------------------------------
# Codex adversarial-review hardening (post-merge, CHAOS-3294): six
# counterexamples the review reproduced against the merged contracts. Each
# test below is the kept, permanent regression form of a fail-before/
# pass-after pair: before the corresponding source fix, the payload here
# validated cleanly (the hole); after the fix, it is rejected. The
# fail-before half was independently re-verified by running each of these
# against the pre-fix ``validators``/``answer``/``compat``/``stream``
# modules (git-stashed) and confirming they passed at that point.
# ---------------------------------------------------------------------------


def test_finding1_denied_frame_cannot_leak_answer_content() -> None:
    """A ``denied`` frame with a subject, a 3/4 completion rate, evidence,
    and source observations intact — the exact Codex counterexample — must
    now be rejected (previously only ``sections``/``facts`` were checked)."""

    payload = deepcopy(positive_fixtures()["dev_answer_frame.v1"])
    payload["public_outcome"] = "denied"
    payload["sections"] = []
    payload["facts"] = []
    # Leave subject_ref, completion (3/4), evidence, and source_observations
    # populated — exactly the review's reproduction.
    with pytest.raises(ValidationError, match="denied"):
        v2.DevAnswerFrame.model_validate(payload)


@pytest.mark.parametrize(
    "case",
    [
        "denied_with_completion",
        "denied_with_readiness",
        "denied_with_metrics",
        "denied_with_comparisons",
        "denied_with_relationship_paths",
        "denied_with_evidence",
        "denied_with_source_observations",
        "denied_with_health_profile_refs",
        "denied_with_finding_refs",
        "denied_with_deficiency_refs",
        "denied_with_subject_identity",
    ],
)
def test_finding1_no_answer_outcome_rejects_each_prohibited_field(case: str) -> None:
    payload = dict(negative_fixtures()["dev_answer_frame.v1"])[case]
    with pytest.raises(ValidationError):
        v2.DevAnswerFrame.model_validate(payload)


@pytest.mark.parametrize(
    "case",
    [
        "narrative_contradicts_number",
        "narrative_contradicts_readiness",
        "narrative_contradicts_subject",
        "narrative_contradicts_recommendation",
    ],
)
def test_finding2_narrative_cannot_contradict_frame(case: str) -> None:
    """A narrative claiming "100% complete, ready, no open work" against a
    frame declaring 75%/not-ready/open-issue — the Codex counterexample,
    split into its four independently-triggering contradiction kinds."""

    payload = dict(negative_fixtures()["dev_answer.v2"])[case]
    with pytest.raises(ValidationError):
        v2.DevAnswerV2.model_validate(payload)


def test_finding3_compat_never_widens_subject_set_to_organization() -> None:
    """A team-cohort (``subject_set_ref``) frame must never project to a v1
    ``DevScope`` with ``direct_scope=ORGANIZATION`` — the old code's "no
    ``subject_ref``" fallback branch silently did exactly that."""

    answer_payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    answer_payload["frame"]["subject_ref"] = None
    answer_payload["frame"]["subject_set_ref"] = "set_team_cohort_01"
    answer = v2.DevAnswerV2.model_validate(answer_payload)
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevErrorV1)
    assert projected.code == "feature_not_enabled"


def test_finding4_answer_run_id_must_match_frame_run_id() -> None:
    payload = dict(negative_fixtures()["dev_answer.v2"])["frame_run_id_mismatch"]
    with pytest.raises(ValidationError, match="run_id"):
        v2.DevAnswerV2.model_validate(payload)


def test_finding4_stream_answer_completed_run_id_must_match_answer_run_id() -> None:
    payloads = stream_fixtures()["invalid_answer_run_id_mismatch"]
    with pytest.raises((ValidationError, ValueError)):
        v2.validate_stream_v2(
            [v2.DevStreamEventV2.model_validate(item) for item in payloads]
        )


def test_finding5_stream_resolution_ledger_cannot_be_rewritten_across_updates() -> None:
    """Two ``resolution.updated`` events, the second rewriting the first's
    entry rather than appending — previously validated independently since
    ``validate_ledger_extends`` was never applied between snapshots."""

    payloads = stream_fixtures()["invalid_ledger_rewrite"]
    with pytest.raises((ValidationError, ValueError)):
        v2.validate_stream_v2(
            [v2.DevStreamEventV2.model_validate(item) for item in payloads]
        )


def test_finding6_stream_rejects_premature_done() -> None:
    """``run.started, done, error, done`` — a premature ``done`` before the
    real terminal result — used to validate because the old check only
    looked at whether the *last* event was ``done``."""

    payloads = stream_fixtures()["invalid_premature_done"]
    with pytest.raises((ValidationError, ValueError)):
        v2.validate_stream_v2(
            [v2.DevStreamEventV2.model_validate(item) for item in payloads]
        )


def test_finding6_stream_rejects_duplicate_done() -> None:
    payloads = stream_fixtures()["invalid_duplicate_done"]
    with pytest.raises((ValidationError, ValueError)):
        v2.validate_stream_v2(
            [v2.DevStreamEventV2.model_validate(item) for item in payloads]
        )


# ---------------------------------------------------------------------------
# Round-2 adversarial review: three of the round-1 closures were bypassed via
# adjacent variants, so each fix below is a *class* closure rather than a
# patch. Every test in this section was first run against the pre-fix source
# and observed to pass with the adversarial payload accepted; each now
# observes rejection. The closure argument for each class — the partition of
# the input space and why every cell is covered — is in
# ``docs/contribute/architecture/ask-dev-contracts-v2.md`` and the
# ``validators`` module docstring.
# ---------------------------------------------------------------------------


def _reaches_str(annotation: object, seen: frozenset[object] = frozenset()) -> bool:
    """True when any type reachable from ``annotation`` is a string."""

    if annotation is None or annotation is type(None) or annotation in seen:
        return False
    if isinstance(annotation, type):
        if issubclass(annotation, str):
            return True
        if issubclass(annotation, BaseModel):
            return any(
                _reaches_str(field.annotation, seen | {annotation})
                for field in annotation.model_fields.values()
            )
        return False
    return any(_reaches_str(arg, seen) for arg in get_args(annotation))


_NO_ANSWER_POLICIES = (
    (v2.DevAnswerFrame, v2.NO_ANSWER_FRAME_FIELD_POLICY),
    (v2.DevAnswerV2, v2.NO_ANSWER_ANSWER_FIELD_POLICY),
)


@pytest.mark.parametrize(
    ("model", "policy"),
    _NO_ANSWER_POLICIES,
    ids=lambda value: getattr(value, "__name__", ""),
)
def test_round2_no_answer_policy_classifies_every_field(
    model: type[v2.ContractModelV2], policy: dict[str, v2.NoAnswerFieldPolicy]
) -> None:
    """Finding 1, closure: the enumeration is derived from the model itself.

    The round-1 fix was a denylist of prohibited field names, which review
    walked around via the fields it did not name. The policy is now total
    over ``model_fields``: a field added without a classification fails here
    (and fails the package import, via
    ``validators.assert_no_answer_policy_is_total``).
    """

    assert set(policy) == set(model.model_fields)
    for name, rule in policy.items():
        annotation = model.model_fields[name].annotation
        if rule is v2.NoAnswerFieldPolicy.NON_TEXT:
            # NON_TEXT is not a way to excuse a text field from the policy:
            # it is only valid when the field reaches no string at all.
            assert not _reaches_str(annotation), name
        elif _reaches_str(annotation):
            assert rule in {
                v2.NoAnswerFieldPolicy.ABSENT,
                v2.NoAnswerFieldPolicy.CANONICAL,
                v2.NoAnswerFieldPolicy.CLOSED_VOCABULARY,
                v2.NoAnswerFieldPolicy.IDENTIFIER,
                v2.NoAnswerFieldPolicy.SELF_VALIDATED,
            }, name


def _no_answer_frame_policy_cells() -> dict[v2.NoAnswerFieldPolicy, set[str]]:
    cells: dict[v2.NoAnswerFieldPolicy, set[str]] = {}
    for name, rule in v2.NO_ANSWER_FRAME_FIELD_POLICY.items():
        cells.setdefault(rule, set()).add(name)
    return cells


_IDENTIFIER_FIELD_NAME = re.compile(r"(^|_)(id|ids)$")
_SERVER_HANDLE_PATTERN = get_args(v2.ServerHandle)[1].pattern


def _field_pattern(field: Any) -> str | None:
    for meta in field.metadata:
        pattern = getattr(meta, "pattern", None)
        if pattern:
            return pattern
    return None


#: Grammars an identifier may carry instead of being a server mint, each
#: pinned to a real producer: the plan registry token and the evidence
#: service's HMAC handle (``evidence_service.issue`` -> ``ev1_`` + 40 hex).
_PINNED_ID_GRAMMARS = frozenset(
    {
        get_args(v2.PlanRegistryID)[1].pattern,
        get_args(v2.EvidenceHandle)[1].pattern,
        _SERVER_HANDLE_PATTERN,
    }
)


def _element_pattern(model: type[BaseModel], name: str) -> str | None:
    """The constraint pattern of a collection field's element type."""

    for arg in get_args(model.model_fields[name].annotation):
        for meta in get_args(arg)[1:]:
            pattern = getattr(meta, "pattern", None)
            if pattern:
                return pattern
    return None


def _is_server_handle(model: type[BaseModel], name: str) -> bool:
    field = model.model_fields[name]
    if _field_pattern(field) == _SERVER_HANDLE_PATTERN:
        return True
    # tuple[ServerHandle, ...] / ServerHandle | None
    for arg in get_args(field.annotation):
        for meta in get_args(arg)[1:]:
            if getattr(meta, "pattern", None) == _SERVER_HANDLE_PATTERN:
                return True
    return False


def _reaches_only_closed_values(model: type[BaseModel], name: str) -> bool:
    """True when the field's own annotation is a closed enum or Literal."""

    annotation = model.model_fields[name].annotation
    if isinstance(annotation, type) and issubclass(annotation, StrEnum):
        return True
    args = get_args(annotation)
    if args and all(isinstance(arg, str) for arg in args):
        return True
    return any(
        isinstance(arg, type) and issubclass(arg, StrEnum)
        for arg in args
        if isinstance(arg, type)
    )


def _models_reachable_on_a_no_answer_projection() -> set[type[BaseModel]]:
    """The models a no-answer answer can actually carry.

    Follows only fields the no-answer policy does *not* classify ``ABSENT``,
    so a model reachable exclusively through an absent field (every one of
    ``DevFrameVersions``, ``DevAnswerFact``, ``DevNarrative``, ...) is
    correctly excluded: nothing in it can appear on a denial.
    """

    reachable: set[type[BaseModel]] = set()
    stack: list[type[BaseModel]] = [v2.DevAnswerV2]
    policies = {
        v2.DevAnswerV2: v2.NO_ANSWER_ANSWER_FIELD_POLICY,
        v2.DevAnswerFrame: v2.NO_ANSWER_FRAME_FIELD_POLICY,
    }
    while stack:
        model = stack.pop()
        if model in reachable:
            continue
        reachable.add(model)
        policy = policies.get(model, {})
        for name, field in model.model_fields.items():
            if policy.get(name) is v2.NoAnswerFieldPolicy.ABSENT:
                continue
            for candidate in (field.annotation, *get_args(field.annotation)):
                if isinstance(candidate, type) and issubclass(candidate, BaseModel):
                    stack.append(candidate)
    return reachable


def test_round4_every_identifier_on_a_denied_envelope_is_an_opaque_handle() -> None:
    """Finding 1, closure: the partition now covers the whole envelope.

    The round-3 claim was scoped to ``DevAnswerFrame`` and asserted against the
    frame policy table alone, so it was *inaccurate at the envelope level*:
    ``DevAnswerV2.answer_id`` and ``conversation_id`` sat one level out, were
    never enumerated, and accepted ``"private/Nightfall"`` on a denied answer.

    This walks the answer envelope itself -- every model a no-answer outcome
    can carry, following only fields the policy does not blank -- and requires
    every identifier cell that survives to be a server-minted opaque handle.
    There are no named exceptions: the residual round 3 recorded is closed.
    """

    reachable = _models_reachable_on_a_no_answer_projection()
    assert reachable == {v2.DevAnswerV2, v2.DevAnswerFrame, v2.DevCoverageV2}

    policies = {
        v2.DevAnswerV2: v2.NO_ANSWER_ANSWER_FIELD_POLICY,
        v2.DevAnswerFrame: v2.NO_ANSWER_FRAME_FIELD_POLICY,
    }
    surviving = [
        (model, name)
        for model in sorted(reachable, key=lambda m: m.__name__)
        for name in model.model_fields
        if _IDENTIFIER_FIELD_NAME.search(name)
        and policies.get(model, {}).get(name) is not v2.NoAnswerFieldPolicy.ABSENT
    ]
    assert {f"{m.__name__}.{n}" for m, n in surviving} == {
        "DevAnswerV2.answer_id",
        "DevAnswerV2.conversation_id",
        "DevAnswerV2.run_id",
        "DevAnswerFrame.frame_id",
        "DevAnswerFrame.run_id",
    }
    unrestricted = [
        f"{model.__name__}.{name}"
        for model, name in surviving
        if not _is_server_handle(model, name)
    ]
    assert unrestricted == []


#: Identifier cells across the whole v2 package that are deliberately *not*
#: server-minted handles, each with the reason it cannot be one. Anything not
#: listed here and not a handle, closed vocabulary, or provenance token fails
#: ``test_round4_every_v2_identifier_is_classified``.
_NON_HANDLE_IDENTIFIER_REASONS: dict[str, str] = {
    # Provider-derived: these name real external entities, so they are
    # subject-derived by construction. All are ABSENT on a no-answer outcome.
    "DevEntityRefV2.entity_id": "provider entity key",
    "DevEntityRefV2.repository_id": "provider entity key",
    "DevEntityRefV2.team_id": "provider entity key",
    "DevEvidenceRefV2.entity_id": "provider entity key",
    "DevEvidenceRefV2.repository_ids": "provider entity key",
    "DevEvidenceRefV2.valid_entity_ids": "provider entity key",
    "DevInvestigationResult.subject_entity_id": "provider entity key",
    "DevRelationshipPath.source_entity_id": "provider entity key",
    "DevRelationshipPath.target_entity_id": "provider entity key",
    "DevScopeV2.organization_id": "provider entity key",
    "DevScopeV2.team_ids": "provider entity key",
    # CHAOS-3295: DevSourceContent's per-source-class fact mirrors. Entity ids
    # name real platform records (PRs, CI runs, deployments, incidents, graph
    # nodes); the field is unreachable from a no-answer frame regardless (see
    # DevSourceObservation.content's docstring).
    "DevCIFactV2.entity_id": "provider entity key",
    "DevDeploymentFactV2.entity_id": "provider entity key",
    "DevGraphEdgeV2.edge_id": "provider entity key",
    "DevGraphEdgeV2.source_entity_id": "provider entity key",
    "DevGraphEdgeV2.target_entity_id": "provider entity key",
    "DevIncidentFactV2.entity_id": "provider entity key",
    "DevObservedChangeV2.entity_id": "provider entity key",
    "DevPullRequestFactV2.entity_id": "provider entity key",
    # Intra-document reference keys: scoped to one document, meaningless
    # outside it, and ABSENT on a no-answer outcome.
    "DevAnswerFact.fact_id": "intra-document key",
    "DevAnswerFact.evidence_ref_ids": "intra-document key",
    "DevAnswerFact.relationship_path_ids": "intra-document key",
    "DevRelationshipPath.path_id": "intra-document key",
    "DevCIFactV2.evidence_ref_ids": "intra-document key",
    "DevDeploymentFactV2.evidence_ref_ids": "intra-document key",
    "DevGraphEdgeV2.evidence_ref_ids": "intra-document key",
    "DevIncidentFactV2.evidence_ref_ids": "intra-document key",
    "DevObservedChangeV2.change_id": "intra-document key",
    "DevPullRequestFactV2.evidence_ref_ids": "intra-document key",
    "DevRequiredChildFactV2.fact_id": "intra-document key",
    "DevRequiredChildFactV2.evidence_ref_ids": "intra-document key",
    "DevStatusFactV2.fact_id": "intra-document key",
    "DevStatusFactV2.evidence_ref_ids": "intra-document key",
    # Client-supplied at the request boundary. The server folds an arbitrary
    # string to a UUID5 (`router._storage_uuid`, applied to exactly these two
    # at router.py:1031/1038), and `DevError.request_id` echoes the same
    # client value back (`body.request_id or header_request_id`), so pinning
    # any of the three to a mint would reject input production accepts.
    "DevMessageRequestV2.request_id": "client-supplied, folded to UUID5",
    "DevMessageRequestV2.client_message_id": "client-supplied, folded to UUID5",
    "DevErrorV2.request_id": "echoes the client-supplied request id",
    "DevAnswerSection.section_id": "intra-document key",
    "DevAnswerSection.fact_ids": "intra-document key",
    "DevMetricRefV2.metric_ref_id": "intra-document key",
    "DevNarrative.referenced_fact_ids": "intra-document key",
    "DevNarrative.referenced_section_ids": "intra-document key",
    "DevReadinessBlock.blocking_fact_ids": "intra-document key",
    "DevSourceObservation.evidence_ref_ids": "intra-document key",
    "DevPlanStepDependency.step_id": "intra-document key",
    # Server-owned rule/adapter registries, versioned tokens rather than mints.
    "DevCompletionBlock.rule_id": "rule registry token",
    "DevReadinessBlock.rule_id": "rule registry token",
    "DevInvestigationPlan.completion_rule_id": "rule registry token",
    "DevSourceRequirement.applicability_rule_id": "rule registry token",
    "DevSourceObservation.adapter_id": "adapter registry token",
    "DevSourceRequirement.adapter_id": "adapter registry token",
    # CHAOS-3302 health-rule governance contracts (contracts_v2.health_rules)
    # -- not part of the no-answer-projection model space (never registered
    # in CONTRACT_MODELS_V2 / reachable from a denied DevAnswerV2), but
    # still subclass ContractModelV2 so this reflection-based sweep reaches
    # them. Their subject/team identifiers name real project/team/portfolio
    # entities, the same category as DevEntityRefV2.entity_id above.
    "DimensionObservation.subject_id": "provider entity key",
    "HealthRuleFinding.subject_id": "provider entity key",
    "TeamQualificationResult.team_id": "provider entity key",
    # CHAOS-3305 operational-deficiency-inventory contracts
    # (contracts_v2.deficiency) -- same category as the CHAOS-3302 rows
    # above: subject identifiers name real project/team/portfolio entities,
    # and evidence_ref_ids is scoped to one inventory document, the same
    # category as DevSourceObservation.evidence_ref_ids above.
    "DeficiencyFinding.subject_id": "provider entity key",
    "OperationalDeficiencyInventory.subject_id": "provider entity key",
    "DeficiencyFinding.evidence_ref_ids": "intra-document key",
}


def test_round4_every_v2_identifier_is_classified() -> None:
    """Finding 1, closure: no identifier cell anywhere is simply unexamined.

    Enumerated from the models rather than from a hand-written list, so a new
    identifier field fails here until it is either a server handle, a closed
    vocabulary, a provenance token, or named above with a reason.
    """

    unclassified: list[str] = []
    for model in _all_v2_contract_models():
        for name in model.model_fields:
            if not _IDENTIFIER_FIELD_NAME.search(name):
                continue
            key = f"{model.__name__}.{name}"
            if _is_server_handle(model, name):
                continue
            if _reaches_only_closed_values(model, name):
                continue
            if _field_pattern(model.model_fields[name]) in _PINNED_ID_GRAMMARS:
                continue
            if _element_pattern(model, name) in _PINNED_ID_GRAMMARS:
                continue
            if key in _NON_HANDLE_IDENTIFIER_REASONS:
                continue
            unclassified.append(key)
    assert unclassified == []

    # The reasons table may not drift into naming fields that no longer exist.
    known = {
        f"{model.__name__}.{name}"
        for model in _all_v2_contract_models()
        for name in model.model_fields
    }
    assert set(_NON_HANDLE_IDENTIFIER_REASONS) <= known


@pytest.mark.parametrize(
    ("target", "cell"),
    [
        ("answer", "answer_id"),
        ("answer", "conversation_id"),
        ("answer", "run_id"),
        ("frame", "frame_id"),
        ("frame", "run_id"),
    ],
)
def test_round4_subject_derived_value_rejected_in_each_envelope_id(
    target: str, cell: str
) -> None:
    """Finding 1: the reproduced payload, per cell, on a denied answer."""

    payload = no_answer_payload("denied")
    if target == "answer":
        payload[cell] = "private/Nightfall"
        if cell == "run_id":
            payload["frame"]["run_id"] = "private/Nightfall"
    else:
        payload["frame"][cell] = "private/Nightfall"
        if cell == "run_id":
            payload["run_id"] = "private/Nightfall"
    with pytest.raises(ValidationError, match=cell):
        v2.DevAnswerV2.model_validate(payload)


@pytest.mark.parametrize(
    ("target", "cell"),
    [
        ("answer", "answer_id"),
        ("answer", "conversation_id"),
        ("answer", "run_id"),
        ("frame", "frame_id"),
        ("frame", "run_id"),
    ],
)
@pytest.mark.parametrize("case", ["upper", "mixed"])
def test_round5_server_handles_are_lowercase_only(
    target: str, cell: str, case: str
) -> None:
    """Finding 2: the grammar must describe what the mint emits.

    ``str(uuid.uuid4())`` is lowercase on every path, so mixed case was a
    grammar admitting values the server never produces. Not a disclosure
    channel -- nothing non-hex fits either way -- but a contract that accepts
    what its own producer cannot emit invites a second, divergent notion of a
    valid handle. Covers every identifier cell a no-answer envelope reaches.
    """

    def mangle(value: str) -> str:
        return value.upper() if case == "upper" else value[:8].upper() + value[8:]

    payload = no_answer_payload("denied")
    if target == "answer":
        payload[cell] = mangle(payload[cell])
        if cell == "run_id":
            payload["frame"]["run_id"] = mangle(payload["frame"]["run_id"])
    else:
        payload["frame"][cell] = mangle(payload["frame"][cell])
        if cell == "run_id":
            payload["run_id"] = mangle(payload["run_id"])
    with pytest.raises(ValidationError, match=cell):
        v2.DevAnswerV2.model_validate(payload)


def test_round5_every_fixture_handle_is_canonical_lowercase() -> None:
    """Finding 2: the goldens themselves must be in the mint's own form."""

    handles = [
        value
        for payload in positive_fixtures().values()
        for value in _walk_strings(payload)
        if re.fullmatch(r"[0-9a-fA-F-]{36}", value)
    ]
    assert handles, "no handle-shaped fixture values found"
    assert all(value == value.lower() for value in handles)


def _walk_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for item in value.values() for s in _walk_strings(item)]
    if isinstance(value, list):
        return [s for item in value for s in _walk_strings(item)]
    return []


def test_round4_request_boundary_accepts_client_shaped_identifiers() -> None:
    """Finding 1, the other direction: do not pin what the client supplies.

    ``request_id`` and ``client_message_id`` are client-supplied and folded to
    a UUID5 server-side (``router._storage_uuid``, router.py:1031/1038), and
    ``DevError.request_id`` echoes the same client value back. Requiring the
    server mint on any of the three would reject input production explicitly
    accepts — the same over-rejection class as the completion gate, so it gets
    the same treatment: an assertion that the loose form still validates.
    """

    payload = deepcopy(positive_fixtures()["dev_message_request.v2"])
    payload["request_id"] = "web-req-2026-07-31-0002"
    payload["client_message_id"] = "web-msg-2026-07-31-0002"
    request = v2.DevMessageRequestV2.model_validate(payload)
    assert request.request_id == "web-req-2026-07-31-0002"

    error = deepcopy(positive_fixtures()["dev_stream_event.v2"])
    error["event"] = "error"
    error["error"] = {
        "schema_version": "dev_error.v1",
        "request_id": "web-req-2026-07-31-0002",
        "code": "source_unavailable",
        "safe_message": "A required source is temporarily unavailable.",
        "retryable": True,
        "remediation": ["Retry after source health recovers."],
    }
    v2.DevStreamEventV2.model_validate(error)

    # ...while the conversation handle on the same request *is* pinned: the
    # router parses it as a path `uuid.UUID` and requires the body to match.
    payload["conversation_id"] = "not-a-conversation-handle"
    with pytest.raises(ValidationError, match="conversation_id"):
        v2.DevMessageRequestV2.model_validate(payload)


def test_round4_evidence_handles_keep_their_hmac_grammar() -> None:
    """Finding 1: evidence handles are a keyed HMAC, not a mint.

    ``evidence_service.issue`` returns ``ev1_`` + 40 hex and ``verify``
    recomputes and ``compare_digest``s it, so the handle is the authorization
    token for dereferencing evidence. A handle that could never verify — or one
    carrying a subject name — cannot reach the wire.
    """

    valid = positive_fixtures()["dev_answer_frame.v1"]["evidence"][0]["evidence_ref_id"]
    assert re.fullmatch(r"ev1_[0-9a-f]{40}", valid)

    for bad in ("private/Nightfall", "ev1_Nightfall", "ev_01", valid.upper()):
        payload = deepcopy(positive_fixtures()["dev_answer_frame.v1"])
        payload["evidence"][0]["evidence_ref_id"] = bad
        payload["facts"][0]["evidence_ref_ids"] = [bad]
        with pytest.raises(ValidationError):
            v2.DevAnswerFrame.model_validate(payload)


def test_round3_denied_projection_admits_no_producer_chosen_string() -> None:
    """Finding 1, closure: the partition of a denied frame's fields, stated.

    Round 2 left two ``IDENTIFIER`` cells that constrained a token's *shape*
    while admitting any well-shaped value, and review round 3 put
    ``"private/Nightfall"`` through both. The partition is now: every field is
    ``ABSENT``, ``CANONICAL`` server copy, ``NON_TEXT``, a ``CLOSED_VOCABULARY``
    the server owns, or delegated to a nested contract with its own policy —
    with exactly two ``IDENTIFIER`` cells left, the correlation handles, which
    are named here so a *third* one cannot be added without this failing.
    """

    cells = _no_answer_frame_policy_cells()
    assert cells.get(v2.NoAnswerFieldPolicy.IDENTIFIER) == {"frame_id", "run_id"}
    assert cells.get(v2.NoAnswerFieldPolicy.CLOSED_VOCABULARY) == {
        "schema_version",
        "public_outcome",
    }
    assert cells.get(v2.NoAnswerFieldPolicy.SELF_VALIDATED) == {"coverage"}
    assert v2.NO_ANSWER_FRAME_FIELD_POLICY["versions"] is v2.NoAnswerFieldPolicy.ABSENT


def test_round3_closed_vocabulary_policy_rejects_a_subject_derived_source() -> None:
    """Finding 1: the projection layer is load-bearing on its own.

    ``DevCoverageV2``'s source lists are the closed ``SourceClass`` enum, so a
    subject-derived name never survives *type* validation — which would make
    the ``CLOSED_VOCABULARY`` classification look covered while doing nothing.
    Constructing the object past validation isolates the policy layer, so a
    later widening of the type (a new adapter, a looser annotation) cannot
    silently reopen the channel on a denial.
    """

    payload = no_answer_payload("denied")["frame"]
    frame = v2.DevAnswerFrame.model_validate(payload)
    smuggled = frame.coverage.model_construct(
        **{
            **dict(frame.coverage),
            "unavailable_required_sources": ("private/Nightfall",),
        }
    )
    forged = frame.model_construct(**{**dict(frame), "coverage": smuggled})

    with pytest.raises(ValueError, match="server-owned vocabulary"):
        v2.validate_no_answer_projection(forged)


def test_round3_identifier_policy_rejects_free_text_past_the_type() -> None:
    """Finding 1: ``IDENTIFIER`` is a runtime predicate, not a type promise.

    The remaining ``IDENTIFIER`` cells are ``OpaqueID``-typed, so the type
    rejects prose first. This isolates the policy the same way the closed
    vocabulary is isolated above, keeping the documented claim proven rather
    than merely asserted.
    """

    frame = v2.DevAnswerFrame.model_validate(no_answer_payload("denied")["frame"])
    forged = frame.model_construct(
        **{**dict(frame), "frame_id": "the restricted Nightfall frame"}
    )

    with pytest.raises(ValueError, match="not free text"):
        v2.validate_no_answer_projection(forged)


def test_round3_no_answer_frame_omits_provenance_but_answered_requires_it() -> None:
    """Finding 1: ``versions`` is dropped, not merely constrained.

    Both directions matter. A no-answer frame must not carry the block (it was
    the channel that carried ``plan_id="private/Nightfall"``), and making the
    field optional must not have made it droppable from a frame that does
    carry content.
    """

    denied = v2.DevAnswerFrame.model_validate(no_answer_payload("denied")["frame"])
    assert denied.versions is None

    answered = deepcopy(positive_fixtures()["dev_answer_frame.v1"])
    answered["versions"] = None
    with pytest.raises(ValidationError, match="requires a versions"):
        v2.DevAnswerFrame.model_validate(answered)


def test_round3_plan_id_and_versions_reject_a_subject_derived_token() -> None:
    """Finding 1: the provenance block's own grammar, on the answered path.

    ``versions`` is absent from a denial, but an answered frame still carries
    it, and it was free-form ``Version`` strings. Every field is now a dotted,
    lowercase, version-suffixed platform token, so a subject-derived
    identifier cannot be spelled in one at all.
    """

    for field in ("plan_id", "plan_version", "interpreter_version", "query_version"):
        payload = deepcopy(positive_fixtures()["dev_answer_frame.v1"])
        payload["versions"][field] = "private/Nightfall"
        with pytest.raises(ValidationError, match=field):
            v2.DevAnswerFrame.model_validate(payload)


def _frame_absent_field_samples() -> dict[str, object]:
    """One populating value per ``ABSENT`` frame field.

    Most are lifted straight out of the exported negative fixtures, so this
    table and the checked-in artifacts cannot disagree about what a
    prohibited field looks like.
    """

    cases = dict(negative_fixtures()["dev_answer_frame.v1"])
    from_case = {
        "subject_ref": "denied_with_subject_identity",
        "clarification_candidates": "denied_with_clarification_candidates",
        "completion": "denied_with_completion",
        "readiness": "denied_with_readiness",
        "metrics": "denied_with_metrics",
        "comparisons": "denied_with_comparisons",
        "relationship_paths": "denied_with_relationship_paths",
        "health_profile_refs": "denied_with_health_profile_refs",
        "finding_refs": "denied_with_finding_refs",
        "deficiency_refs": "denied_with_deficiency_refs",
        "health_findings": "denied_with_health_findings",
        "deficiency_findings": "denied_with_deficiency_findings",
        "conflicts": "denied_with_conflicts",
        "limitations": "denied_with_limitations",
        "source_observations": "denied_with_source_observations",
        "evidence": "denied_with_evidence",
        "safe_follow_up_questions": "denied_with_follow_up_questions",
    }
    samples: dict[str, object] = {
        field: cases[label][field] for field, label in from_case.items()
    }
    samples["versions"] = cases["denied_with_versions"]["versions"]
    samples["subject_set_ref"] = "set_restricted_01"
    samples["sections"] = [
        {"section_id": "summary", "title": "Restricted summary", "fact_ids": []}
    ]
    samples["facts"] = [
        {
            "fact_id": "fact_01",
            "text": "The restricted project has one open issue.",
            "kind": "observed",
            "evidence_ref_ids": [],
            "relationship_path_ids": [],
            "confidence": 1.0,
        }
    ]
    return samples


def test_round2_every_absent_frame_field_is_individually_rejected() -> None:
    """Finding 1, closure: every ``ABSENT`` cell of the partition is covered.

    The sample table must name exactly the ``ABSENT`` fields — a field newly
    classified ``ABSENT`` fails here until it has a payload proving it is
    actually rejected, so classification alone can never stand in for
    enforcement.
    """

    absent_fields = {
        name
        for name, rule in v2.NO_ANSWER_FRAME_FIELD_POLICY.items()
        if rule is v2.NoAnswerFieldPolicy.ABSENT
    }
    samples = _frame_absent_field_samples()
    assert set(samples) == absent_fields

    for field, value in samples.items():
        payload = no_answer_payload("denied")["frame"]
        payload[field] = value
        with pytest.raises(ValidationError, match=re.escape(field)):
            v2.DevAnswerFrame.model_validate(payload)


@pytest.mark.parametrize("outcome", sorted(v2.NO_ANSWER_OUTCOMES))
def test_round2_no_answer_direct_answer_must_be_canonical_copy(outcome: str) -> None:
    """Finding 1: producer-authored copy is replaced, never re-emitted."""

    payload = no_answer_payload(outcome)
    v2.DevAnswerV2.model_validate(payload)  # the canonical form validates

    payload["frame"]["direct_answer"] = (
        "Project Nightfall is 40% complete but you are not on its guild."
    )
    with pytest.raises(ValidationError, match="canonical server copy"):
        v2.DevAnswerV2.model_validate(payload)


@pytest.mark.parametrize("outcome", sorted(v2.NO_ANSWER_OUTCOMES))
def test_round2_no_answer_outcome_cannot_carry_a_narrative(outcome: str) -> None:
    """Finding 1: the narrative is the free-form channel the scrub missed."""

    payload = no_answer_payload(outcome)
    payload["narrative"] = {
        **deepcopy(positive_fixtures()["dev_narrative.v1"]),
        "referenced_fact_ids": [],
        "referenced_section_ids": [],
        "body": ("The project Nightfall exists but is restricted to another guild."),
    }
    with pytest.raises(ValidationError, match="narrative"):
        v2.DevAnswerV2.model_validate(payload)


def test_round2_v1_projection_of_a_no_answer_outcome_carries_no_frame_text() -> None:
    """Finding 1: the v1 boundary was the channel that reached real clients."""

    answer = v2.DevAnswerV2.model_validate(no_answer_payload("denied"))
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevErrorV1)
    assert projected.safe_message == v2.CANONICAL_NO_ANSWER_COPY["denied"]
    assert projected.remediation  # canonical remediation, not frame follow-ups


# --- Finding 2: frozen means immutable, not just non-rebindable -------------


def _all_v2_contract_models() -> list[type[v2.ContractModelV2]]:
    found: set[type[v2.ContractModelV2]] = set()
    stack: list[type[v2.ContractModelV2]] = [v2.ContractModelV2]
    while stack:
        for subclass in stack.pop().__subclasses__():
            if subclass not in found:
                found.add(subclass)
                stack.append(subclass)
    return sorted(found, key=lambda model: model.__name__)


def _has_mutable_collection(annotation: object) -> bool:
    if get_origin(annotation) in {list, set, dict}:
        return True
    return any(_has_mutable_collection(arg) for arg in get_args(annotation))


def test_round2_no_v2_model_has_a_mutable_collection_field() -> None:
    """Finding 2, closure: the whole model space, not the one model reviewed.

    ``ConfigDict(frozen=True)`` only blocks attribute rebinding; a ``list``
    field's contents stay mutable, which review used to clear a validated
    ledger and defeat ``validate_ledger_extends``. Every collection field on
    every v2 contract is a ``tuple``, checked by introspection so a new
    ``list`` field anywhere in the package fails here.
    """

    models = _all_v2_contract_models()
    assert len(models) >= 20  # the walk actually found the model space
    offenders = [
        f"{model.__name__}.{name}"
        for model in models
        for name, field in model.model_fields.items()
        if _has_mutable_collection(field.annotation)
    ]
    assert offenders == []


def test_round2_validated_ledger_cannot_be_emptied_in_place() -> None:
    """Finding 2, regression: the exact mutation used to defeat the baseline."""

    previous = v2.DevResolutionLedger.model_validate(
        positive_fixtures()["dev_resolution_ledger.v1"]
    )
    rewritten_payload = deepcopy(positive_fixtures()["dev_resolution_ledger.v1"])
    rewritten_payload["entries"][0]["outcome"] = "no_authorized_match"
    rewritten_payload["entries"][0]["committed_entity_ref"] = None
    rewritten_payload["entries"][0]["repository_attribution"] = None
    rewritten = v2.DevResolutionLedger.model_validate(rewritten_payload)

    with pytest.raises(ValueError, match="cannot rewrite or erase"):
        v2.validate_ledger_extends(previous, rewritten)

    with pytest.raises(AttributeError):
        previous.entries.clear()  # type: ignore[attr-defined]
    with pytest.raises(TypeError):
        previous.mention_ids[0] = "mention_rewritten"  # type: ignore[index]

    # The baseline is intact, so the rewrite is still rejected.
    with pytest.raises(ValueError, match="cannot rewrite or erase"):
        v2.validate_ledger_extends(previous, rewritten)


def _reachable_models(model: type[BaseModel]) -> set[type[BaseModel]]:
    """Every model type reachable from ``model``'s fields, at any depth."""

    found: set[type[BaseModel]] = set()

    def walk(annotation: object) -> None:
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            if annotation in found:
                return
            found.add(annotation)
            for field in annotation.model_fields.values():
                walk(field.annotation)
            return
        for arg in get_args(annotation):
            walk(arg)

    for field in model.model_fields.values():
        walk(field.annotation)
    return found


def test_round3_no_mutable_collection_anywhere_in_the_v2_closure() -> None:
    """Finding 2, closure: the whole reachable object graph, not the v2 layer.

    ``test_round2_no_v2_model_has_a_mutable_collection_field`` checked only
    models that subclass ``ContractModelV2``, and round 2 recorded the v1
    models embedded in them as an acknowledged boundary. Review round 3 showed
    that boundary sat *inside* the graph the v2 validators had just certified::

        frame.coverage.unavailable_required_sources.append("private/Nightfall")

    so it was never a boundary, it was a hole. The predicate is now: no model
    reachable from any v2 contract, at any depth and whether or not it is a v2
    model, declares a mutable collection. A newly embedded v1 model with a
    ``list`` field fails here rather than reopening the seam.
    """

    reachable: set[type[BaseModel]] = set()
    for model in _all_v2_contract_models():
        reachable.add(model)
        reachable |= _reachable_models(model)

    assert len(reachable) >= 25  # the walk actually found the object graph
    offenders = sorted(
        f"{model.__name__}.{name}"
        for model in reachable
        for name, field in model.model_fields.items()
        if _has_mutable_collection(field.annotation)
    )
    assert offenders == []


def test_round3_validated_frame_cannot_be_mutated_after_the_fact() -> None:
    """Finding 2, regression: the exact post-validation mutations review used."""

    answer = v2.DevAnswerV2.model_validate(
        deepcopy(positive_fixtures()["dev_answer.v2"])
    )
    frame = answer.frame

    with pytest.raises(AttributeError):
        frame.coverage.unavailable_required_sources.append(  # type: ignore[attr-defined]
            "private/Nightfall"
        )
    with pytest.raises(AttributeError):
        frame.evidence[0].repository_ids.append("repo_restricted")  # type: ignore[attr-defined]
    with pytest.raises(TypeError):
        frame.evidence[0].valid_entity_ids[0] = "item_rewritten"  # type: ignore[index]

    # ...and the serialized wire output is unchanged by the attempts.
    assert (
        answer.model_dump(mode="json")["frame"]["coverage"][
            "unavailable_required_sources"
        ]
        == []
    )


def test_round3_v1_projection_still_emits_plain_v1_collections() -> None:
    """Finding 2: the mirrors must not leak their tuples into v1 output.

    A mirror ``isinstance``-passes as its v1 original, so pydantic would take
    one into a v1-typed field untouched and then serialize a ``tuple`` where
    v1 declares a ``list``. The projector converts explicitly; this is what
    proves it does.
    """

    answer = v2.DevAnswerV2.model_validate(
        deepcopy(positive_fixtures()["dev_answer.v2"])
    )
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevAnswerV1)
    assert type(projected.coverage) is DevCoverage
    assert isinstance(projected.coverage.unavailable_required_sources, list)
    assert all(type(item) is DevEvidenceRef for item in projected.evidence)
    for item in projected.evidence:
        assert isinstance(item.repository_ids, list)


# --- Finding 3: narrative claims bound to the facts they narrate ------------


@pytest.mark.parametrize(
    "case",
    [
        "narrative_unrelated_comparison_number",
        "narrative_substring_subject",
        "narrative_unbound_recommendation",
        "narrative_completion_number_out_of_context",
    ],
)
def test_round2_narrative_binding_rejects_each_bypass(case: str) -> None:
    """Finding 3: the variants that walked around the earlier checks."""

    payload = dict(negative_fixtures()["dev_answer.v2"])[case]
    with pytest.raises(ValidationError):
        v2.DevAnswerV2.model_validate(payload)


_NARRATIVE_BINDING_CASES: dict[str, str] = {
    "validate_narrative_numeric_containment": "narrative_unrelated_comparison_number",
    "validate_narrative_subject_claim": "narrative_substring_subject",
    "validate_narrative_recommendation_claim": "narrative_unbound_recommendation",
}


@pytest.mark.parametrize("validator_name", sorted(_NARRATIVE_BINDING_CASES))
def test_round2_each_narrative_binding_rule_is_individually_load_bearing(
    monkeypatch: pytest.MonkeyPatch, validator_name: str
) -> None:
    """Finding 3: each bypass is attributable to exactly one new rule.

    Without this, a single over-broad rule could be rejecting all three
    payloads and the other two would read as covered while doing nothing.
    """

    cases = dict(negative_fixtures()["dev_answer.v2"])
    target_case = _NARRATIVE_BINDING_CASES[validator_name]

    with pytest.raises(ValidationError):
        v2.DevAnswerV2.model_validate(cases[target_case])

    monkeypatch.setattr(validators_module, validator_name, lambda *_a, **_k: None)
    v2.DevAnswerV2.model_validate(cases[target_case])  # only this guard rejected it

    for other_case in _NARRATIVE_BINDING_CASES.values():
        if other_case == target_case:
            continue
        with pytest.raises(ValidationError):
            v2.DevAnswerV2.model_validate(cases[other_case])


def test_round2_no_answer_projection_is_what_rejects_the_denied_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 1: the whole ``denied_*`` family is attributable to guard (f).

    Every one of them passes once the projection is disabled — so none is
    incidentally rejected by a different validator — while the four
    named-guardrail fixtures still reject.
    """

    frame_cases = negative_fixtures()["dev_answer_frame.v1"]
    denied_cases = [
        (label, payload)
        for label, payload in frame_cases
        if label.startswith("denied_with_")
    ]
    assert len(denied_cases) >= 16

    for _label, payload in denied_cases:
        with pytest.raises(ValidationError):
            v2.DevAnswerFrame.model_validate(payload)

    monkeypatch.setattr(
        validators_module, "validate_no_answer_projection", lambda *_a, **_k: None
    )
    for _label, payload in denied_cases:
        v2.DevAnswerFrame.model_validate(payload)

    for label, payload in frame_cases:
        if label.startswith("denied_with_"):
            continue
        with pytest.raises(ValidationError):
            v2.DevAnswerFrame.model_validate(payload)


def test_round2_narrative_may_cite_the_frames_own_completion_percentage() -> None:
    """Finding 3, non-over-rejection: a true completion claim still validates."""

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = "Repository dev-health is 75% complete."
    v2.DevAnswerV2.model_validate(payload)


def test_round2_narrative_may_cite_a_comparison_it_names() -> None:
    """Finding 3, non-over-rejection: comparison values are label-bound, not banned.

    The same value (100) that is rejected in
    ``narrative_unrelated_comparison_number`` is accepted here, because this
    sentence names the comparison it comes from. That pair is what makes the
    numeric rule *binding* rather than merely stricter.
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["frame"]["comparisons"] = [
        {
            "label": "Review throughput",
            "current_value": 100.0,
            "comparison_value": 82.0,
            "unit": "count",
        }
    ]
    payload["narrative"]["body"] = (
        "Repository dev-health recorded review throughput of 100 this window."
    )
    v2.DevAnswerV2.model_validate(payload)


def test_round2_narrative_may_recommend_a_fact_it_references() -> None:
    """Finding 3, non-over-rejection: grounded recommendation prose validates."""

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["frame"]["facts"].append(
        {
            "fact_id": "fact_rec",
            "text": "Add a second reviewer to the release checklist.",
            "kind": "recommendation",
            "evidence_ref_ids": ["ev1_a2bc440cf82f6979884d6d486dacdc900744d04b"],
            "relationship_path_ids": [],
            "confidence": 0.8,
        }
    )
    payload["frame"]["sections"][0]["fact_ids"].append("fact_rec")
    payload["narrative"]["referenced_fact_ids"] = ["fact_01", "fact_rec"]
    payload["narrative"]["body"] = (
        "Repository dev-health is on track. We recommend adding a second "
        "reviewer to the release checklist."
    )
    v2.DevAnswerV2.model_validate(payload)


def test_round3_completion_numbers_are_not_a_global_narrative_token_pool() -> None:
    """Finding 3, acceptance counterexample: the pool legitimized any sentence.

    ``4`` is the frame's completion *denominator*. Round 2 unioned it into one
    pool offered to every sentence, so a claim about open security incidents
    was grounded by a number that says nothing about incidents. Completion
    values are now admitted only where a completion claim is actually made —
    and the pair below is what makes that binding rather than merely stricter:
    the same number, in a sentence that does make a completion claim, is
    accepted (``test_round2_narrative_may_cite_the_frames_own_completion_percentage``).
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = (
        "Repository dev-health has 4 open security incidents."
    )
    with pytest.raises(ValidationError, match="cites number"):
        v2.DevAnswerV2.model_validate(payload)


def test_round3_narrative_may_name_a_subject_whose_identity_contains_a_number() -> None:
    """Finding 3, over-rejection counterexample: the inverse of the same bug.

    A pool that admits numbers by membership also *refuses* them by
    membership: a subject genuinely named ``project-42`` could not be named,
    because 42 appeared in no fact. The subject's canonical identity tokens
    are server-committed, so they are admitted directly.
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["frame"]["subject_ref"]["entity_id"] = "project_42"
    payload["frame"]["subject_ref"]["display_label"] = "full-chaos/project-42"
    payload["frame"]["relationship_paths"][0]["source_entity_id"] = "project_42"
    payload["narrative"]["body"] = (
        "project-42 is on track, with one required child issue still open."
    )
    v2.DevAnswerV2.model_validate(payload)


def test_round3_frame_free_text_no_longer_grounds_a_narrative_number() -> None:
    """Finding 3: ``direct_answer`` and friends are out of the admission set.

    They were in the round-2 pool. A number appearing in the frame's own
    free text grounds nothing about the sentence citing it — the narrative
    must reference the fact that carries it. The pair proves the rule is
    doing the work: the same sentence is accepted once a fact carrying the
    number is referenced.
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["frame"]["limitations"] = ["Only 9 of the required sources responded."]
    payload["frame"]["public_outcome"] = "answered_with_gaps"
    payload["public_outcome"] = "answered_with_gaps"
    payload["outcome_display_label"] = "Answered with some gaps"
    payload["narrative"]["body"] = "Repository dev-health saw 9 responding sources."
    with pytest.raises(ValidationError, match="cites number"):
        v2.DevAnswerV2.model_validate(payload)

    payload["frame"]["facts"].append(
        {
            "fact_id": "fact_sources",
            "text": "9 of the required sources responded.",
            "kind": "observed",
            "evidence_ref_ids": ["ev1_a2bc440cf82f6979884d6d486dacdc900744d04b"],
            "relationship_path_ids": [],
            "confidence": 1.0,
        }
    )
    payload["narrative"]["referenced_fact_ids"] = ["fact_01", "fact_sources"]
    v2.DevAnswerV2.model_validate(payload)


#: Truthful and false narrations of the *same* 3/4 completion block, one pair
#: per supported completion verb. The positive half is the point: round 3
#: gated admission on a five-word regex, so every truthful phrasing outside
#: that list was rejected — over-rejection on the positive path, which is the
#: defect this wave exists to prevent. The negative half is what stops the fix
#: from being a blanket "admit any number near a completion word".
_COMPLETION_NARRATION_PAIRS: dict[str, tuple[str, str]] = {
    "progress": (
        "Repository dev-health has made 75% progress.",
        "Repository dev-health has made 90% progress.",
    ),
    "passed": (
        "Repository dev-health has passed 3 of 4 required checks.",
        "Repository dev-health has passed 4 of 5 required checks.",
    ),
    "closed": (
        "Repository dev-health has closed 3 of 4 required issues.",
        "Repository dev-health has closed 4 of 4 required issues.",
    ),
    "delivered": (
        "Repository dev-health has delivered 75% of the required work.",
        "Repository dev-health has delivered 100% of the required work.",
    ),
    "remaining": (
        "Repository dev-health has 25% remaining.",
        "Repository dev-health has 40% remaining.",
    ),
    "ratio_slash": (
        "Repository dev-health has 3/4 required checks closed.",
        "Repository dev-health has 2/4 required checks closed.",
    ),
    "decimal_rate": (
        "Repository dev-health sits at a completion rate of 0.75.",
        "Repository dev-health sits at a completion rate of 0.9.",
    ),
    # Round 5: the complement is a real rendering of the block, so the false
    # half here is not an unsupported number — it is the *supported* number
    # cited with the wrong polarity, which round 4 admitted.
    "polarity_completed": (
        "Repository dev-health has completed 75%.",
        "Repository dev-health has completed 25%.",
    ),
    "polarity_remaining": (
        "Repository dev-health has 25% remaining.",
        "Repository dev-health has 75% remaining.",
    ),
    "polarity_decimal": (
        "Repository dev-health has a completion rate of 0.75.",
        "Repository dev-health has a completion rate of 0.25.",
    ),
    # Round 5: ordinary typography. The false halves keep the same typography
    # so the pair isolates the value, not the punctuation.
    "spaced_percent": (
        "Repository dev-health is 75 % complete.",
        "Repository dev-health is 90 % complete.",
    ),
    "hyphenated_ratio": (
        "Repository dev-health has passed 3-of-4 required checks.",
        "Repository dev-health has passed 2-of-4 required checks.",
    ),
    "out_of_ratio": (
        "Repository dev-health has passed 3 out of 4 required checks.",
        "Repository dev-health has passed 3 out of 5 required checks.",
    ),
}

#: Codex's round-5 payloads, verbatim, kept separate from the generated pairs
#: so the exact reproduced strings stay greppable against the review.
_ROUND5_REPORTED_PAYLOADS: dict[str, tuple[str, bool]] = {
    "false_completed_complement": ("Repository dev-health has completed 25%.", False),
    "false_decimal_complement": (
        "Repository dev-health has a completion rate of 0.25.",
        False,
    ),
    "truthful_spaced_percent": ("Repository dev-health is 75 % complete.", True),
    "truthful_hyphenated_ratio": (
        "Repository dev-health has passed 3-of-4 required checks.",
        True,
    ),
}


@pytest.mark.parametrize("case", sorted(_ROUND5_REPORTED_PAYLOADS))
def test_round5_reported_completion_payloads(case: str) -> None:
    """Finding 1: the four payloads review reproduced, exactly as reported.

    Two were accepted and should not have been (the complement cited as
    completed, in both percent and decimal form); two were rejected and should
    not have been (a space before ``%``, a hyphenated ratio). All four were
    observed in the wrong state against the pre-fix source.
    """

    body, should_validate = _ROUND5_REPORTED_PAYLOADS[case]
    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = body
    if should_validate:
        v2.DevAnswerV2.model_validate(payload)
    else:
        with pytest.raises(ValidationError):
            v2.DevAnswerV2.model_validate(payload)


def test_round5_polarity_is_only_enforced_where_it_is_decidable() -> None:
    """Finding 1, scope: polarity applies where the two sides actually differ.

    At a 1/2 completion block the rate and its complement are the same number,
    so there is no wrong answer to give and neither phrasing is rejected. A
    sentence claiming both directions is ambiguous prose rather than a
    contradiction, and is likewise left alone -- over-rejecting it would be
    the same defect this round is fixing.
    """

    half = deepcopy(positive_fixtures()["dev_answer.v2"])
    half["frame"]["completion"] = {
        "numerator": 1,
        "denominator": 2,
        "rate": 0.5,
        "calculable": True,
        "rule_id": "actual_completion",
        "rule_version": "actual_completion.v1",
    }
    for body in (
        "Repository dev-health has completed 50%.",
        "Repository dev-health has 50% remaining.",
    ):
        payload = deepcopy(half)
        payload["narrative"]["body"] = body
        v2.DevAnswerV2.model_validate(payload)

    both = deepcopy(positive_fixtures()["dev_answer.v2"])
    both["narrative"]["body"] = (
        "Repository dev-health has 25% remaining of the work to be completed."
    )
    v2.DevAnswerV2.model_validate(both)


@pytest.mark.parametrize("variant", sorted(_COMPLETION_NARRATION_PAIRS))
def test_round4_completion_narration_accepts_truth_and_rejects_falsehood(
    variant: str,
) -> None:
    """Finding 2, both directions: the same block, narrated truthfully or not."""

    truthful, false = _COMPLETION_NARRATION_PAIRS[variant]

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = truthful
    v2.DevAnswerV2.model_validate(payload)

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = false
    with pytest.raises(ValidationError):
        v2.DevAnswerV2.model_validate(payload)


def test_round4_completion_admission_does_not_depend_on_vocabulary() -> None:
    """Finding 2, closure: admission is by citation shape, not by keyword.

    This sentence contains no word from ``_COMPLETION_CLAIM_STEMS`` at all and
    still validates, because ``75%`` is a completion proportion written with
    its unit. That is what makes the fix a removal of the keyword gate rather
    than a longer keyword list.
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = "Repository dev-health sits at 75% of the target."
    assert not validators_module._COMPLETION_CLAIM_PATTERN.search(
        payload["narrative"]["body"]
    )
    v2.DevAnswerV2.model_validate(payload)


def test_round4_bare_completion_count_needs_a_referenced_fact() -> None:
    """Finding 2, the stated limitation and its escape hatch.

    A bare completion count is genuinely ambiguous — nothing distinguishes
    "3 required checks" from "3 open incidents" — so it is admitted only via
    the block's own ratio rendering or a fact the narrative references. The
    pair proves the escape hatch works rather than leaving the limitation as
    an unqualified refusal.
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = (
        "Repository dev-health has 3 required checks outstanding."
    )
    with pytest.raises(ValidationError, match="cites number"):
        v2.DevAnswerV2.model_validate(payload)

    payload["frame"]["facts"].append(
        {
            "fact_id": "fact_checks",
            "text": "3 required checks are outstanding.",
            "kind": "observed",
            "evidence_ref_ids": ["ev1_a2bc440cf82f6979884d6d486dacdc900744d04b"],
            "relationship_path_ids": [],
            "confidence": 1.0,
        }
    )
    payload["narrative"]["referenced_fact_ids"] = ["fact_01", "fact_checks"]
    v2.DevAnswerV2.model_validate(payload)


def test_round4_completion_counts_need_the_ratio_not_mere_co_occurrence() -> None:
    """Finding 2: admitting the pair on co-occurrence would reopen round 3.

    "4 open security incidents and 3 unresolved alerts" contains both the
    numerator and the denominator and grounds neither, so the counts are
    admitted only where the sentence renders the block's own ratio.
    """

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = (
        "Repository dev-health has 4 open security incidents and 3 unresolved alerts."
    )
    with pytest.raises(ValidationError, match="cites number"):
        v2.DevAnswerV2.model_validate(payload)


def test_round2_narrative_may_name_the_subject_by_its_short_form() -> None:
    """Finding 3, non-over-rejection: canonical identity, not exact string."""

    payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    payload["narrative"]["body"] = (
        "dev-health is on track, with one required child issue still open."
    )
    v2.DevAnswerV2.model_validate(payload)


# --- Finding 4: one lifecycle invariant, stated once ------------------------


def test_round2_stream_rejects_duplicate_run_started() -> None:
    payloads = stream_fixtures()["invalid_duplicate_start"]
    with pytest.raises(ValueError, match="exactly one run.started"):
        v2.validate_stream_v2(
            [v2.DevStreamEventV2.model_validate(item) for item in payloads]
        )


@pytest.mark.parametrize(
    "case",
    [
        "invalid_duplicate_start",
        "invalid_duplicate_done",
        "invalid_premature_done",
        "invalid_duplicate_terminal",
        "invalid_missing_done",
    ],
)
def test_round2_every_lifecycle_marker_misplacement_is_rejected(case: str) -> None:
    """Finding 4, closure: the partition is (marker) x (wrong count | wrong position).

    Each of the three lifecycle markers — ``run.started``, the terminal
    result, ``done`` — is required to occur exactly once at exactly one
    index, so both cells of the partition fail for every marker. The cases
    here are the reproduced instances; the invariant covers the rest by
    construction rather than by enumeration.
    """

    payloads = stream_fixtures()[case]
    with pytest.raises((ValidationError, ValueError)):
        v2.validate_stream_v2(
            [v2.DevStreamEventV2.model_validate(item) for item in payloads]
        )
