"""Structural guards on the corpus itself.

These catch the failure mode where the corpus *looks* complete -- 21 cases, 7
questions, a pile of oracles -- while some case has nothing measuring it. A
corpus case with no oracle reads as coverage in every summary and is coverage
in none of them.
"""

from __future__ import annotations

import pytest

from ..corpus.cases import CORPUS_CASES, CORPUS_CASES_BY_ID
from ..corpus.ground_truth import GROUND_TRUTH
from ..corpus.oracles import ALL_ORACLES, ORACLES_BY_ID
from ..corpus.questions import EVALUATION_QUESTIONS, QUESTIONS_BY_ID
from ..harness.contracts import QueryMode, QuestionClass


def test_all_twenty_one_prd_cases_are_present() -> None:
    assert len(CORPUS_CASES) == 21
    assert sorted(case.prd_index for case in CORPUS_CASES) == list(range(1, 22))


def test_every_corpus_case_is_exercised_by_a_real_oracle() -> None:
    """No case may sit in the corpus with nothing measuring it."""
    orphans = [case.case_id for case in CORPUS_CASES if not case.exercised_by]
    assert not orphans, f"corpus cases with no oracle: {orphans}"

    dangling = {
        case.case_id: [
            oracle_id
            for oracle_id in case.exercised_by
            if oracle_id not in ORACLES_BY_ID
        ]
        for case in CORPUS_CASES
    }
    dangling = {k: v for k, v in dangling.items() if v}
    assert not dangling, f"cases naming oracles that do not exist: {dangling}"


def test_every_oracle_names_only_real_corpus_cases() -> None:
    dangling = {
        oracle.oracle_id: [
            case_id
            for case_id in oracle.corpus_case_ids
            if case_id not in CORPUS_CASES_BY_ID
        ]
        for oracle in ALL_ORACLES
    }
    dangling = {k: v for k, v in dangling.items() if v}
    assert not dangling, f"oracles naming cases that do not exist: {dangling}"


def test_every_evaluation_question_has_at_least_one_oracle() -> None:
    covered = {oracle.question_id for oracle in ALL_ORACLES}
    missing = [
        q.question_id for q in EVALUATION_QUESTIONS if q.question_id not in covered
    ]
    assert not missing, f"evaluation questions with no oracle: {missing}"


def test_every_oracle_belongs_to_a_declared_question() -> None:
    for oracle in ALL_ORACLES:
        assert oracle.question_id in QUESTIONS_BY_ID, (
            f"{oracle.oracle_id} references unknown question {oracle.question_id}"
        )


def test_oracle_class_matches_its_question_class() -> None:
    """An oracle cannot be reported under a class its question does not hold.

    Per-class reporting is only meaningful if the class on the result row is
    the class the question was actually assigned.
    """
    mismatches = [
        (
            oracle.oracle_id,
            oracle.question_class,
            QUESTIONS_BY_ID[oracle.question_id].question_class,
        )
        for oracle in ALL_ORACLES
        if oracle.question_class
        is not QUESTIONS_BY_ID[oracle.question_id].question_class
    ]
    assert not mismatches, f"oracle/question class mismatches: {mismatches}"


def test_all_three_classes_are_populated() -> None:
    """A class with no questions makes its per-class row meaningless.

    If this ever fails, the ADR's per-class table has an empty row that a
    reader will misread as "clean".
    """
    populated = {q.question_class for q in EVALUATION_QUESTIONS}
    missing = [k.value for k in QuestionClass if k not in populated]
    assert not missing, f"evaluation-question classes with no questions: {missing}"


@pytest.mark.parametrize(
    "oracle",
    [o for o in ALL_ORACLES if o.query.query_mode is QueryMode.AS_OF],
    ids=lambda o: o.oracle_id,
)
def test_every_as_of_oracle_pins_its_axis(oracle) -> None:
    """PRD §10, restated as a test rather than a convention."""
    assert oracle.query.axis is not None
    assert oracle.query.as_of is not None, (
        f"{oracle.oracle_id} is an as-of query with no as_of instant"
    )


def test_axis_pair_actually_pins_opposite_axes() -> None:
    valid = ORACLES_BY_ID["O2_blocking_valid"]
    observed = ORACLES_BY_ID["O2_blocking_observed"]
    assert valid.query.as_of == observed.query.as_of, (
        "an axis pair must ask the same instant, or it is two different "
        "questions rather than one question on two axes"
    )
    assert valid.query.axis is not observed.query.axis


def test_every_ground_truth_fact_names_a_real_case() -> None:
    dangling = {
        fact.fact_key: [c for c in fact.for_cases if c not in CORPUS_CASES_BY_ID]
        for fact in GROUND_TRUTH
    }
    dangling = {k: v for k, v in dangling.items() if v}
    assert not dangling, f"ground-truth facts naming unknown cases: {dangling}"


def test_adversarial_ground_truth_is_marked() -> None:
    """Planted attack material must be flagged, or the selector will serve it.

    ``ground_truth.select`` excludes adversarial facts by default. A fact
    planted as an attack but left unmarked silently becomes part of every
    golden answer, which would make the security oracles pass by construction.
    """
    attack_cases = {
        "C14_prompt_injection",
        "C15_cross_tenant_near_duplicate",
        "C17_retrieval_manipulation",
        "C18_entity_linking_poisoning",
    }
    undecided = [
        fact.fact_key
        for fact in GROUND_TRUTH
        if set(fact.for_cases) & attack_cases and fact.is_adversarial == fact.is_control
    ]
    assert not undecided, (
        "facts planted for an attack case must declare themselves either "
        f"adversarial or control, never both and never neither: {undecided}"
    )

    # And every attack case must actually have attack material, or the case
    # is a title with nothing behind it.
    for case_id in attack_cases:
        planted = [
            f.fact_key
            for f in GROUND_TRUTH
            if case_id in f.for_cases and f.is_adversarial
        ]
        assert planted, f"attack case {case_id} plants no adversarial fact"


def test_no_oracle_is_assertion_free() -> None:
    """Belt-and-braces over the Oracle constructor's own guard."""
    for oracle in ALL_ORACLES:
        assert oracle.must_include or oracle.must_exclude or oracle.coverage, (
            f"{oracle.oracle_id} asserts nothing"
        )
