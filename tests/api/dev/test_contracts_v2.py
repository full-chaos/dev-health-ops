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

from copy import deepcopy

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev import contracts_v2 as v2
from dev_health_ops.api.dev.contract_fixtures import (
    positive_fixtures as positive_fixtures_v1,
)
from dev_health_ops.api.dev.contract_fixtures_v2 import (
    NOW,
    negative_fixtures,
    positive_fixtures,
    stream_fixtures,
)
from dev_health_ops.api.dev.contracts import (
    CONTRACT_MODELS,
    AnswerStatus,
    DevScope,
    DevTimeRange,
)
from dev_health_ops.api.dev.contracts import DevAnswer as DevAnswerV1
from dev_health_ops.api.dev.contracts import DevError as DevErrorV1
from dev_health_ops.api.dev.contracts_v2 import validators as validators_module
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
        "mention_id": "mention_02",
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
    extended_payload["mention_ids"].append("mention_02")
    extended_payload["entries"].append(_second_entry())
    extended = v2.DevResolutionLedger.model_validate(extended_payload)

    v2.validate_ledger_extends(previous, extended)  # must not raise


def test_ledger_extension_rejects_rewriting_a_prior_entry() -> None:
    previous = v2.DevResolutionLedger.model_validate(
        positive_fixtures()["dev_resolution_ledger.v1"]
    )
    extended_payload = deepcopy(positive_fixtures()["dev_resolution_ledger.v1"])
    extended_payload["mention_ids"].append("mention_02")
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
    extended_payload["mention_ids"].append("mention_02")
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
}


@pytest.mark.parametrize("validator_name", sorted(_FRAME_VALIDATOR_CASES))
def test_disabling_one_frame_validator_flips_only_its_own_fixture(
    monkeypatch: pytest.MonkeyPatch, validator_name: str
) -> None:
    schema_version, target_case = _FRAME_VALIDATOR_CASES[validator_name]
    all_cases = negative_fixtures()[schema_version]
    target_payload = dict(all_cases)[target_case]

    # Baseline: rejected with every validator active.
    with pytest.raises(ValidationError):
        v2.DevAnswerFrame.model_validate(target_payload)

    monkeypatch.setattr(validators_module, validator_name, lambda *_a, **_k: None)

    # The targeted fixture now passes: the disabled guard was the only thing
    # rejecting it.
    v2.DevAnswerFrame.model_validate(target_payload)

    # Every other case for this schema is a different guardrail and must
    # still be rejected — proves the mutation is attributable to exactly one
    # validator, not a global bypass.
    for other_case, other_payload in all_cases:
        if other_case == target_case:
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
        "unavailable_required_sources": ["deployment_health"],
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
    answer_payload = deepcopy(positive_fixtures()["dev_answer.v2"])
    answer_payload["frame"]["public_outcome"] = outcome
    answer_payload["frame"]["sections"] = []
    answer_payload["frame"]["facts"] = []
    answer_payload["frame"]["completion"] = None
    answer_payload["frame"]["readiness"] = None
    # A NO_ANSWER_OUTCOMES frame must carry nothing beyond the bare outcome
    # (validate_no_answer_content_leaks, CHAOS-3294 adversarial-review fix).
    answer_payload["frame"]["metrics"] = []
    answer_payload["frame"]["comparisons"] = []
    answer_payload["frame"]["relationship_paths"] = []
    answer_payload["frame"]["evidence"] = []
    answer_payload["frame"]["source_observations"] = []
    answer_payload["frame"]["health_profile_refs"] = []
    answer_payload["frame"]["finding_refs"] = []
    answer_payload["frame"]["deficiency_refs"] = []
    answer_payload["frame"]["subject_ref"] = None
    answer_payload["frame"]["subject_set_ref"] = None
    answer_payload["public_outcome"] = outcome
    answer_payload["outcome_display_label"] = {
        "not_found": "Not found",
        "temporarily_unavailable": "Temporarily unavailable",
        "unsupported": "Not supported yet",
        "denied": "Not permitted",
        "failed": "Something went wrong",
    }[outcome]
    answer_payload["narrative"] = None
    answer = v2.DevAnswerV2.model_validate(answer_payload)
    projected = v2.project_answer_v2_to_v1(
        answer, organization_id="org_fullchaos", time_range=_time_range_for_scope()
    )
    assert isinstance(projected, DevErrorV1)
    assert projected.code == expected_code


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
