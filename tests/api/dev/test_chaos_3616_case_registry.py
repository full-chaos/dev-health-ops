"""CHAOS-3616: the case registry is total, mapped, and honest about skips.

``validate_case_registry`` runs at import. These tests add the properties an
import guard cannot state on its own: that the registry covers every corpus
family and every frozen question family, that a case names the defect it
catches rather than the data it holds, and — the one that matters most — that
a skipped case can never discharge a coverage obligation.

Each guard is exercised by planting the defect it exists to catch and
watching it fire. A registry validator nobody has seen reject anything is a
validator nobody has tested.
"""

from __future__ import annotations

import dataclasses

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ALL_QUESTION_FAMILY_IDS,
    QUESTION_FAMILY_REGISTRY,
)
from dev_health_ops.api.dev.investigation_corpus import cases as cases_module
from dev_health_ops.api.dev.investigation_corpus.cases import (
    ALL_CASE_IDS,
    CASE_REGISTRY,
    REQUIRED_CORPUS_TOPICS,
    AnswerDisposition,
    CaseDisposition,
    CorpusFamily,
    authored_cases,
    validate_case_registry,
)


def _registry_without(case_id: str) -> dict[str, object]:
    return {key: value for key, value in CASE_REGISTRY.items() if key != case_id}


# --------------------------------------------------------------------------
# Totality
# --------------------------------------------------------------------------


def test_registry_validates() -> None:
    validate_case_registry()


def test_every_corpus_family_has_a_case() -> None:
    covered = {case.corpus_family for case in CASE_REGISTRY.values()}
    assert covered == set(CorpusFamily)


def test_every_frozen_question_family_has_a_case() -> None:
    """A family with no case is a family the trial will report nothing about.

    The corrected trial reports per question family; an unpopulated family
    would render as an empty row that reads like a clean sheet.
    """

    covered = {case.question_family for case in CASE_REGISTRY.values()}
    missing = sorted(str(item) for item in set(ALL_QUESTION_FAMILY_IDS) - covered)
    assert not missing, f"question families with no corpus case: {missing}"


def test_every_required_topic_is_claimed_by_an_authored_case() -> None:
    claimed: set[str] = set()
    for case in authored_cases():
        claimed.update(case.topics)
    unclaimed = sorted(set(REQUIRED_CORPUS_TOPICS) - claimed)
    assert not unclaimed, (
        f"required corpus topics claimed by no AUTHORED case: {unclaimed}. A "
        "topic claimed only by a skipped case reads as covered and is not."
    )


def test_every_case_names_a_defect_rather_than_its_data() -> None:
    """'Checks that X works' is a wish; the failure mode is the case."""

    for case in CASE_REGISTRY.values():
        assert len(case.catches.strip()) >= 80, case.case_id
        assert "an arm" in case.catches.casefold() or "nothing, because" in (
            case.catches.casefold()
        ), (
            f"{case.case_id} does not describe what an arm would do wrong; "
            f"got: {case.catches[:120]!r}"
        )


def test_every_case_maps_to_a_shape_its_family_permits() -> None:
    for case in CASE_REGISTRY.values():
        family = QUESTION_FAMILY_REGISTRY[case.question_family]
        assert case.comparison_shape in family.permitted_comparison_shapes, case.case_id


def test_follow_up_cases_reference_a_declared_prior_turn() -> None:
    follow_ups = [
        case for case in CASE_REGISTRY.values() if case.follows_case_id is not None
    ]
    assert follow_ups, (
        "no multi-turn case is registered, so conversational-reference "
        "resolution is scored on nothing"
    )
    for case in follow_ups:
        assert case.follows_case_id in CASE_REGISTRY


def test_the_corpus_exercises_every_answer_disposition() -> None:
    """All four answer shapes must appear, or the scoring cannot tell them apart.

    ``QUALIFIED`` and ``UNAVAILABLE`` in particular are the two halves of the
    staffing rule: without both, an arm that always hedges and an arm that
    always refuses would score identically.
    """

    seen = {case.expected_answer for case in CASE_REGISTRY.values()}
    assert seen == set(AnswerDisposition)


def test_both_halves_of_the_staffing_rule_are_present() -> None:
    """A qualified capacity answer and a genuinely unsupported one.

    P05 and P06 are each other's control. If only one existed, an arm could
    satisfy the corpus by adopting a single fixed behaviour.
    """

    qualified = CASE_REGISTRY["P05_allocation_absent_still_supportable"]
    unavailable = CASE_REGISTRY["P06_no_evidence_for_staffing_conclusion"]
    assert qualified.expected_answer is AnswerDisposition.QUALIFIED
    assert unavailable.expected_answer is AnswerDisposition.UNAVAILABLE
    assert qualified.question_family is unavailable.question_family


# --------------------------------------------------------------------------
# Dispositions are loud
# --------------------------------------------------------------------------


def test_non_authored_cases_state_a_substantive_reason() -> None:
    skipped = [
        case
        for case in CASE_REGISTRY.values()
        if case.disposition is not CaseDisposition.AUTHORED
    ]
    assert skipped, (
        "no case carries a non-authored disposition, so the disposition "
        "machinery is untested and the first unmeasurable case will be deleted "
        "rather than declared"
    )
    for case in skipped:
        assert len(case.disposition_reason.strip()) >= 80, case.case_id
        assert not case.topics, (
            f"{case.case_id} is {case.disposition} but claims required topics; "
            "a skip must never discharge a coverage obligation"
        )


def test_the_unmeasurable_case_names_the_blocking_issue() -> None:
    case = CASE_REGISTRY["X01_historical_cohort_membership_delta"]
    assert case.disposition is CaseDisposition.UNMEASURABLE
    assert "CHAOS-3569" in case.disposition_reason


def test_the_not_authorable_case_names_the_prohibition() -> None:
    case = CASE_REGISTRY["X02_person_free_capacity_denominator"]
    assert case.disposition is CaseDisposition.NOT_AUTHORABLE
    assert "person" in case.disposition_reason.casefold()


# --------------------------------------------------------------------------
# The guards reject what they claim to
# --------------------------------------------------------------------------


def test_an_unclaimed_required_topic_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    extended = dict(REQUIRED_CORPUS_TOPICS)
    extended["team.topic_nobody_covers"] = "a topic the issue requires and nobody wrote"
    monkeypatch.setattr(cases_module, "REQUIRED_CORPUS_TOPICS", extended)
    with pytest.raises(RuntimeError, match="claimed by no"):
        validate_case_registry()


def test_a_skipped_case_claiming_a_topic_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The single most dangerous shape: a skip that reads as coverage."""

    case = CASE_REGISTRY["T01_clearly_struggling_team"]
    silently_skipped = dataclasses.replace(
        case,
        disposition=CaseDisposition.UNMEASURABLE,
        disposition_reason=(
            "A reason long enough to satisfy the length floor but attached to a "
            "case that still claims its topics, which is the shape under test."
        ),
    )
    registry = dict(CASE_REGISTRY)
    registry[case.case_id] = silently_skipped
    monkeypatch.setattr(cases_module, "CASE_REGISTRY", registry)
    with pytest.raises(RuntimeError, match="must never discharge a coverage"):
        validate_case_registry()


def test_a_skip_without_a_reason_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    case = CASE_REGISTRY["X01_historical_cohort_membership_delta"]
    unexplained = dataclasses.replace(case, disposition_reason="blocked")
    registry = dict(CASE_REGISTRY)
    registry[case.case_id] = unexplained
    monkeypatch.setattr(cases_module, "CASE_REGISTRY", registry)
    with pytest.raises(RuntimeError, match="unexamined skip"):
        validate_case_registry()


def test_a_case_whose_shape_its_family_forbids_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A case no legal packet could answer would score arm failure for a corpus bug."""

    case = CASE_REGISTRY["S03_shared_dependency_portfolio_risk"]
    illegal = dataclasses.replace(
        case, comparison_shape=cases_module.ComparisonShape.SINGULAR_SUBJECT
    )
    registry = dict(CASE_REGISTRY)
    registry[case.case_id] = illegal
    monkeypatch.setattr(cases_module, "CASE_REGISTRY", registry)
    with pytest.raises(RuntimeError, match="no legal packet could answer"):
        validate_case_registry()


def test_two_cases_with_the_same_question_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identical prompts make both cases' scores unattributable."""

    first = CASE_REGISTRY["H01_acronym_resolution"]
    second = CASE_REGISTRY["H02_old_and_current_name"]
    registry = dict(CASE_REGISTRY)
    registry[second.case_id] = dataclasses.replace(second, question=first.question)
    monkeypatch.setattr(cases_module, "CASE_REGISTRY", registry)
    with pytest.raises(RuntimeError, match="repeats a question"):
        validate_case_registry()


def test_an_authored_case_claiming_nothing_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = CASE_REGISTRY["T06_healthy_despite_noisy_metric"]
    untagged = dataclasses.replace(case, topics=())
    registry = dict(CASE_REGISTRY)
    registry[case.case_id] = untagged
    monkeypatch.setattr(cases_module, "CASE_REGISTRY", registry)
    with pytest.raises(RuntimeError, match="claims no required topic"):
        validate_case_registry()


def test_registry_ids_are_unique() -> None:
    assert len(set(ALL_CASE_IDS)) == len(ALL_CASE_IDS)
