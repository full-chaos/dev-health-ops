"""Proof that every oracle rejects the behaviour it claims to catch.

This is the suite that makes the rest of the trial believable. For each
oracle it builds a *golden* response -- exactly what the corpus says a correct
arm returns -- asserts the oracle passes it, then mutates that response one
fault at a time and asserts the oracle fails **in the assertion that claims to
catch that fault**.

A fault that cannot be applied to a given oracle is reported, not skipped:
``test_every_fault_mode_applies_somewhere`` fails if any registered fault mode
never applies anywhere, because a mutation that silently no-ops looks exactly
like a mutation that was caught.
"""

from __future__ import annotations

import dataclasses

import pytest

from ..corpus.oracles import ALL_ORACLES, ORACLES_BY_ID
from ..harness.contracts import (
    ArmOutcome,
    ArmResponse,
    ClaimKind,
    FactFlags,
    Invalidation,
    SourceCoverage,
    TemporalFact,
)
from ..harness.faults import FAULT_MODES, FaultApplication
from ..harness.oracle import Oracle, Verdict
from .golden import golden_response

ARM = "golden-reference"


@pytest.mark.parametrize("oracle", ALL_ORACLES, ids=lambda o: o.oracle_id)
def test_golden_response_passes(oracle: Oracle) -> None:
    """The oracle must accept a correct answer.

    On its own this proves nothing -- an oracle with no real assertions also
    passes. It is the necessary half of the pair whose *other* half is the
    fault-mode tests below.
    """
    result = oracle.evaluate(golden_response(oracle, ARM))
    assert result.verdict is Verdict.PASS, (
        f"{oracle.oracle_id} rejected its own golden response: "
        f"{[(a.assertion_id, a.detail) for a in result.assertions if not a.ok]}"
    )


@pytest.mark.parametrize("oracle", ALL_ORACLES, ids=lambda o: o.oracle_id)
@pytest.mark.parametrize("fault", FAULT_MODES, ids=lambda f: f.fault_id)
def test_fault_mode_is_caught_by_its_own_assertion(oracle: Oracle, fault) -> None:
    """Where a fault applies, the oracle must fail -- and fail in the right place.

    Checking only "it failed" would let a mutation die in an unrelated
    assertion and still be scored as caught, which proves nothing about the
    invariant the fault was aimed at.
    """
    golden = golden_response(oracle, ARM)
    outcome = fault.apply(oracle, golden)
    if outcome.application is FaultApplication.INAPPLICABLE:
        pytest.skip(f"{fault.fault_id} does not apply to {oracle.oracle_id}")

    result = oracle.evaluate(outcome.response)
    assert result.verdict.is_failure, (
        f"{oracle.oracle_id} PASSED despite fault {fault.fault_id} "
        f"({fault.description}); the assertion that claims to catch it "
        f"({fault.expected_assertion_id}) is not doing so"
    )
    failed = result.failed_assertion_ids()
    assert any(
        assertion_id.split(":", 1)[0] == fault.expected_assertion_id
        for assertion_id in failed
    ), (
        f"{oracle.oracle_id} failed on {failed} but fault {fault.fault_id} "
        f"claims to be caught by {fault.expected_assertion_id!r}; the "
        f"mutation died somewhere else, so it proves nothing about that "
        f"invariant"
    )


def test_every_fault_mode_applies_to_at_least_one_oracle() -> None:
    """No registered fault may be inert across the whole corpus.

    An inert fault is worse than a missing one: the parametrised suite above
    reports it as a skip, and a wall of skips reads as coverage.
    """
    inert = []
    for fault in FAULT_MODES:
        applied_anywhere = any(
            fault.apply(oracle, golden_response(oracle, ARM)).application
            is FaultApplication.APPLIED
            for oracle in ALL_ORACLES
        )
        if not applied_anywhere:
            inert.append(fault.fault_id)
    assert not inert, f"fault modes that never apply to any oracle: {inert}"


def test_measurement_never_ran_fails_every_oracle() -> None:
    """The one fault that must be universal.

    Every other fault is allowed to be inapplicable somewhere. This one is
    not: an unmeasured oracle must never read as a pass, whatever it asserts.
    """
    for oracle in ALL_ORACLES:
        result = oracle.evaluate(ArmResponse.not_run(ARM, "stack unavailable"))
        assert result.verdict is Verdict.NOT_MEASURED, (
            f"{oracle.oracle_id} returned {result.verdict} for an arm that "
            "was never run"
        )
        assert result.verdict.is_failure


def test_not_measured_is_not_silently_equal_to_pass() -> None:
    """Guards the Verdict semantics the whole report depends on."""
    assert Verdict.NOT_MEASURED.is_failure
    assert Verdict.FAIL.is_failure
    assert not Verdict.PASS.is_failure


def test_oracle_rejects_construction_with_no_assertions() -> None:
    """An oracle that cannot fail must not be constructible."""
    template = ORACLES_BY_ID["O7_valid"]
    with pytest.raises(ValueError, match="cannot fail"):
        Oracle(
            oracle_id="O_empty",
            question_id="Q7",
            question_class=template.question_class,
            query=template.query,
            rationale="deliberately empty",
        )


def test_as_of_oracle_cannot_omit_axis() -> None:
    """PRD §10: an unpinned as-of question has two different right answers."""
    template = ORACLES_BY_ID["O2_blocking_valid"]
    with pytest.raises(ValueError, match="axis"):
        dataclasses.replace(
            template.query, axis=None
        )  # TemporalContextQuery guards it at construction


def test_direction_reversal_is_not_a_near_miss() -> None:
    """A reversed edge must fail, and must say so.

    Direction preservation is a §16 hard gate, and a reversed relationship is
    the most plausible-looking wrong answer an arm can give -- every entity is
    correct, only the meaning is inverted.
    """
    oracle = ORACLES_BY_ID["O3_supersession"]
    golden = golden_response(oracle, ARM)
    head = golden.facts[0]
    reversed_response = dataclasses.replace(
        golden,
        facts=(
            dataclasses.replace(
                head, subject_ref=head.object_ref, object_ref=head.subject_ref
            ),
            *golden.facts[1:],
        ),
    )
    result = oracle.evaluate(reversed_response)
    assert result.verdict is Verdict.FAIL
    detail = " ".join(a.detail for a in result.assertions if not a.ok)
    assert "REVERSED" in detail, (
        "a reversed edge must be reported as a direction failure, not as a "
        f"generic miss: {detail}"
    )


def test_observed_fact_without_provenance_fails_every_oracle() -> None:
    """§16 gate applies everywhere, not only where someone requested it."""
    for oracle in ALL_ORACLES:
        golden = golden_response(oracle, ARM)
        observed = [
            (i, f)
            for i, f in enumerate(golden.facts)
            if f.claim_kind is ClaimKind.OBSERVED
        ]
        if not observed:
            continue
        index, fact = observed[0]
        facts = list(golden.facts)
        facts[index] = dataclasses.replace(fact, evidence_refs=(), source_event_refs=())
        result = oracle.evaluate(dataclasses.replace(golden, facts=tuple(facts)))
        assert "provenance_closure" in result.failed_assertion_ids(), (
            f"{oracle.oracle_id} accepted an observed fact that closes to nothing"
        )


def test_axis_pair_cannot_both_pass_with_one_answer() -> None:
    """The structural heart of the axis-pair case.

    An arm that ignores ``axis`` returns one set for both questions. This
    test proves no single set satisfies both oracles, so such an arm is
    guaranteed to fail one of them -- the test cannot be defeated by a lucky
    ranking.
    """
    valid = ORACLES_BY_ID["O2_blocking_valid"]
    observed = ORACLES_BY_ID["O2_blocking_observed"]

    for source, other in ((valid, observed), (observed, valid)):
        single_answer = golden_response(source, ARM)
        # Re-label the query so the axis echo assertion is not what fails;
        # we want to prove the CONTENT is incompatible.
        relabelled = dataclasses.replace(single_answer, query=other.query)
        result = other.evaluate(relabelled)
        assert result.verdict is Verdict.FAIL, (
            f"{source.oracle_id}'s answer satisfied {other.oracle_id}; the "
            "axis pair does not actually distinguish the two axes"
        )


def test_unavailable_outcome_is_not_satisfiable_by_answering() -> None:
    """Outage cases must not be passable by returning cached results."""
    oracle = ORACLES_BY_ID["O4_prior_attempts_graph_outage"]
    golden = golden_response(oracle, ARM)
    assert oracle.evaluate(golden).verdict is Verdict.PASS

    answered = dataclasses.replace(
        golden,
        outcome=ArmOutcome.ANSWERED,
        facts=(
            TemporalFact(
                fact_id="tf_from_cache",
                subject_ref=oracle.query.subjects[0],
                predicate="touched",
                object_ref=oracle.query.subjects[0],
                observed_at=oracle.query.as_of or _ANY_TIME,
                claim_kind=ClaimKind.OBSERVED,
                projection_version="temporal-projector.v1",
                evidence_refs=("ev1_cached",),
                flags=FactFlags(),
            ),
        ),
    )
    assert oracle.evaluate(answered).verdict is Verdict.FAIL


def test_coverage_gap_must_be_declared_not_merely_empty() -> None:
    """Silent emptiness is the failure mode C16 exists for."""
    oracle = ORACLES_BY_ID["O1_ci_prior_attempts_squash"]
    golden = golden_response(oracle, ARM)
    assert oracle.evaluate(golden).verdict is Verdict.PASS

    undeclared = dataclasses.replace(golden, source_coverage={})
    result = oracle.evaluate(undeclared)
    assert result.verdict is Verdict.FAIL
    assert any(
        a.assertion_id.startswith("coverage:") for a in result.assertions if not a.ok
    )


def test_invalidation_provenance_cannot_be_laundered() -> None:
    """PRD §6.3: an observed fact must not carry an unexplained endpoint."""
    oracle = ORACLES_BY_ID["O3_supersession"]
    golden = golden_response(oracle, ARM)
    closed = [(i, f) for i, f in enumerate(golden.facts) if f.valid_to is not None]
    assert closed, "O3's golden response must contain a closed validity window"
    index, fact = closed[0]
    facts = list(golden.facts)
    facts[index] = dataclasses.replace(fact, invalidated_by=None)
    result = oracle.evaluate(dataclasses.replace(golden, facts=tuple(facts)))
    assert "provenance_closure" in result.failed_assertion_ids()

    # And an INFERRED invalidation on an observed fact must not satisfy an
    # oracle demanding an observed one.
    facts[index] = dataclasses.replace(
        fact,
        invalidated_by=Invalidation(
            refs=("sevt_llm_judgement",),
            invalidation_claim_kind=ClaimKind.INFERRED,
        ),
    )
    result = oracle.evaluate(dataclasses.replace(golden, facts=tuple(facts)))
    assert result.verdict is Verdict.FAIL


def test_source_coverage_type_is_used_not_bypassed() -> None:
    """Cheap guard that the golden builder produces real coverage objects."""
    oracle = ORACLES_BY_ID["O1_ci_prior_attempts_squash"]
    golden = golden_response(oracle, ARM)
    for entry in golden.source_coverage.values():
        assert isinstance(entry, SourceCoverage)


_ANY_TIME = __import__("datetime").datetime(
    2026, 7, 31, tzinfo=__import__("datetime").UTC
)
