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

from ..corpus import ground_truth as gt
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
from ..harness.faults import FAULT_MODES, FaultApplication, hide_coverage_gap
from ..harness.oracle import Oracle, Verdict
from .golden import _adversarial_last, golden_response

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


#: Every (fault_id, oracle_id) pair currently INAPPLICABLE, pinned explicitly
#: so the 197-pair skip set can only change by a conscious edit here.
#:
#: Without a pin, the parametrised suite above just reports these as skips --
#: and if an oracle or fault-mode change silently made a previously-APPLIED
#: pair inapplicable, the only visible symptom is one more skip in a wall
#: that already has 197 of them, which reads as coverage regardless. This
#: manifest turns that silent shrink into a named, reviewable diff.
#:
#: To regenerate after a deliberate corpus/fault-mode change:
#:   for fault in FAULT_MODES:
#:       for oracle in ALL_ORACLES:
#:           outcome = fault.apply(oracle, golden_response(oracle, ARM))
#:           if outcome.application is FaultApplication.INAPPLICABLE:
#:               print((fault.fault_id, oracle.oracle_id))
_PINNED_INAPPLICABLE_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        ("answer_the_other_axis", "O1_ci_prior_attempts"),
        ("answer_the_other_axis", "O1_ci_prior_attempts_squash"),
        ("answer_the_other_axis", "O1_ci_prior_attempts_stale"),
        ("answer_the_other_axis", "O3_supersession"),
        ("answer_the_other_axis", "O3_supersession_deterministic_only"),
        ("answer_the_other_axis", "O3_supersession_extraction_down"),
        ("answer_the_other_axis", "O4_prior_attempts"),
        ("answer_the_other_axis", "O4_prior_attempts_after_redaction"),
        ("answer_the_other_axis", "O4_prior_attempts_after_revocation"),
        ("answer_the_other_axis", "O4_prior_attempts_graph_outage"),
        ("answer_the_other_axis", "O4_prior_attempts_manipulated"),
        ("answer_the_other_axis", "O5_conflicts"),
        ("answer_the_other_axis", "O5_conflicts_injected"),
        ("answer_the_other_axis", "O5_conflicts_poisoned"),
        ("answer_the_other_axis", "O6_recurring_pattern"),
        ("answer_the_other_axis", "O7_unpinned"),
        ("answer_through_outage", "O1_ci_prior_attempts"),
        ("answer_through_outage", "O1_ci_prior_attempts_squash"),
        ("answer_through_outage", "O1_ci_prior_attempts_stale"),
        ("answer_through_outage", "O2_blocking_observed"),
        ("answer_through_outage", "O2_blocking_valid"),
        ("answer_through_outage", "O3_supersession"),
        ("answer_through_outage", "O3_supersession_deterministic_only"),
        ("answer_through_outage", "O3_supersession_extraction_down"),
        ("answer_through_outage", "O4_prior_attempts"),
        ("answer_through_outage", "O4_prior_attempts_after_redaction"),
        ("answer_through_outage", "O4_prior_attempts_after_revocation"),
        ("answer_through_outage", "O4_prior_attempts_manipulated"),
        ("answer_through_outage", "O5_conflicts"),
        ("answer_through_outage", "O5_conflicts_injected"),
        ("answer_through_outage", "O5_conflicts_poisoned"),
        ("answer_through_outage", "O6_recurring_pattern"),
        ("answer_through_outage", "O7_null_valid_from"),
        ("answer_through_outage", "O7_unpinned"),
        ("answer_through_outage", "O7_valid"),
        ("cite_opening_evidence_as_invalidation", "O1_ci_prior_attempts"),
        ("cite_opening_evidence_as_invalidation", "O1_ci_prior_attempts_squash"),
        ("cite_opening_evidence_as_invalidation", "O1_ci_prior_attempts_stale"),
        ("cite_opening_evidence_as_invalidation", "O2_blocking_observed"),
        ("cite_opening_evidence_as_invalidation", "O2_blocking_valid"),
        ("cite_opening_evidence_as_invalidation", "O3_supersession_deterministic_only"),
        ("cite_opening_evidence_as_invalidation", "O3_supersession_extraction_down"),
        ("cite_opening_evidence_as_invalidation", "O4_prior_attempts"),
        ("cite_opening_evidence_as_invalidation", "O4_prior_attempts_after_redaction"),
        ("cite_opening_evidence_as_invalidation", "O4_prior_attempts_after_revocation"),
        ("cite_opening_evidence_as_invalidation", "O4_prior_attempts_graph_outage"),
        ("cite_opening_evidence_as_invalidation", "O4_prior_attempts_manipulated"),
        ("cite_opening_evidence_as_invalidation", "O5_conflicts"),
        ("cite_opening_evidence_as_invalidation", "O5_conflicts_injected"),
        ("cite_opening_evidence_as_invalidation", "O5_conflicts_poisoned"),
        ("cite_opening_evidence_as_invalidation", "O6_recurring_pattern"),
        ("cite_opening_evidence_as_invalidation", "O7_null_valid_from"),
        ("cite_opening_evidence_as_invalidation", "O7_unpinned"),
        ("cite_opening_evidence_as_invalidation", "O7_valid"),
        ("clear_required_flag", "O1_ci_prior_attempts"),
        ("clear_required_flag", "O1_ci_prior_attempts_squash"),
        ("clear_required_flag", "O2_blocking_observed"),
        ("clear_required_flag", "O2_blocking_valid"),
        ("clear_required_flag", "O3_supersession"),
        ("clear_required_flag", "O3_supersession_deterministic_only"),
        ("clear_required_flag", "O3_supersession_extraction_down"),
        ("clear_required_flag", "O4_prior_attempts"),
        ("clear_required_flag", "O4_prior_attempts_after_redaction"),
        ("clear_required_flag", "O4_prior_attempts_after_revocation"),
        ("clear_required_flag", "O4_prior_attempts_graph_outage"),
        ("clear_required_flag", "O4_prior_attempts_manipulated"),
        ("clear_required_flag", "O5_conflicts_poisoned"),
        ("clear_required_flag", "O6_recurring_pattern"),
        ("clear_required_flag", "O7_null_valid_from"),
        ("clear_required_flag", "O7_unpinned"),
        ("clear_required_flag", "O7_valid"),
        ("downgrade_observed_to_inferred", "O1_ci_prior_attempts_squash"),
        ("downgrade_observed_to_inferred", "O1_ci_prior_attempts_stale"),
        ("downgrade_observed_to_inferred", "O3_supersession"),
        ("downgrade_observed_to_inferred", "O3_supersession_deterministic_only"),
        ("downgrade_observed_to_inferred", "O3_supersession_extraction_down"),
        ("downgrade_observed_to_inferred", "O4_prior_attempts_after_revocation"),
        ("downgrade_observed_to_inferred", "O4_prior_attempts_graph_outage"),
        ("downgrade_observed_to_inferred", "O4_prior_attempts_manipulated"),
        ("downgrade_observed_to_inferred", "O5_conflicts"),
        ("downgrade_observed_to_inferred", "O5_conflicts_injected"),
        ("downgrade_observed_to_inferred", "O7_unpinned"),
        ("emit_forbidden_fact", "O1_ci_prior_attempts_squash"),
        ("emit_forbidden_fact", "O1_ci_prior_attempts_stale"),
        ("emit_forbidden_fact", "O3_supersession"),
        ("emit_forbidden_fact", "O4_prior_attempts_graph_outage"),
        ("emit_forbidden_fact", "O4_prior_attempts_manipulated"),
        ("emit_forbidden_fact", "O5_conflicts"),
        ("emit_forbidden_fact", "O7_null_valid_from"),
        ("emit_forbidden_fact", "O7_unpinned"),
        ("hide_source_coverage_gap", "O1_ci_prior_attempts"),
        ("hide_source_coverage_gap", "O1_ci_prior_attempts_stale"),
        ("hide_source_coverage_gap", "O2_blocking_observed"),
        ("hide_source_coverage_gap", "O2_blocking_valid"),
        ("hide_source_coverage_gap", "O3_supersession"),
        ("hide_source_coverage_gap", "O4_prior_attempts"),
        ("hide_source_coverage_gap", "O4_prior_attempts_after_redaction"),
        ("hide_source_coverage_gap", "O4_prior_attempts_after_revocation"),
        ("hide_source_coverage_gap", "O4_prior_attempts_manipulated"),
        ("hide_source_coverage_gap", "O5_conflicts"),
        ("hide_source_coverage_gap", "O5_conflicts_injected"),
        ("hide_source_coverage_gap", "O5_conflicts_poisoned"),
        ("hide_source_coverage_gap", "O6_recurring_pattern"),
        ("hide_source_coverage_gap", "O7_null_valid_from"),
        ("hide_source_coverage_gap", "O7_unpinned"),
        ("hide_source_coverage_gap", "O7_valid"),
        ("leak_out_of_subject_fact", "O1_ci_prior_attempts"),
        ("leak_out_of_subject_fact", "O1_ci_prior_attempts_stale"),
        ("leak_out_of_subject_fact", "O3_supersession"),
        ("omit_expected_evidence", "O1_ci_prior_attempts_squash"),
        ("omit_expected_evidence", "O3_supersession_deterministic_only"),
        ("omit_expected_evidence", "O3_supersession_extraction_down"),
        ("omit_expected_evidence", "O4_prior_attempts_graph_outage"),
        ("restore_redacted_source_ref", "O1_ci_prior_attempts"),
        ("restore_redacted_source_ref", "O1_ci_prior_attempts_squash"),
        ("restore_redacted_source_ref", "O1_ci_prior_attempts_stale"),
        ("restore_redacted_source_ref", "O2_blocking_observed"),
        ("restore_redacted_source_ref", "O2_blocking_valid"),
        ("restore_redacted_source_ref", "O3_supersession"),
        ("restore_redacted_source_ref", "O3_supersession_deterministic_only"),
        ("restore_redacted_source_ref", "O3_supersession_extraction_down"),
        ("restore_redacted_source_ref", "O4_prior_attempts"),
        ("restore_redacted_source_ref", "O4_prior_attempts_after_revocation"),
        ("restore_redacted_source_ref", "O4_prior_attempts_graph_outage"),
        ("restore_redacted_source_ref", "O4_prior_attempts_manipulated"),
        ("restore_redacted_source_ref", "O5_conflicts"),
        ("restore_redacted_source_ref", "O5_conflicts_injected"),
        ("restore_redacted_source_ref", "O5_conflicts_poisoned"),
        ("restore_redacted_source_ref", "O6_recurring_pattern"),
        ("restore_redacted_source_ref", "O7_null_valid_from"),
        ("restore_redacted_source_ref", "O7_unpinned"),
        ("restore_redacted_source_ref", "O7_valid"),
        ("reverse_edge_direction", "O1_ci_prior_attempts_squash"),
        ("reverse_edge_direction", "O3_supersession_deterministic_only"),
        ("reverse_edge_direction", "O3_supersession_extraction_down"),
        ("reverse_edge_direction", "O4_prior_attempts_graph_outage"),
        ("rewind_indexing_watermark", "O1_ci_prior_attempts_stale"),
        ("rewind_indexing_watermark", "O4_prior_attempts_graph_outage"),
        ("smuggle_redacted_ref_via_evidence", "O1_ci_prior_attempts"),
        ("smuggle_redacted_ref_via_evidence", "O1_ci_prior_attempts_squash"),
        ("smuggle_redacted_ref_via_evidence", "O1_ci_prior_attempts_stale"),
        ("smuggle_redacted_ref_via_evidence", "O2_blocking_observed"),
        ("smuggle_redacted_ref_via_evidence", "O2_blocking_valid"),
        ("smuggle_redacted_ref_via_evidence", "O3_supersession"),
        ("smuggle_redacted_ref_via_evidence", "O3_supersession_deterministic_only"),
        ("smuggle_redacted_ref_via_evidence", "O3_supersession_extraction_down"),
        ("smuggle_redacted_ref_via_evidence", "O4_prior_attempts"),
        ("smuggle_redacted_ref_via_evidence", "O4_prior_attempts_after_revocation"),
        ("smuggle_redacted_ref_via_evidence", "O4_prior_attempts_graph_outage"),
        ("smuggle_redacted_ref_via_evidence", "O4_prior_attempts_manipulated"),
        ("smuggle_redacted_ref_via_evidence", "O5_conflicts"),
        ("smuggle_redacted_ref_via_evidence", "O5_conflicts_injected"),
        ("smuggle_redacted_ref_via_evidence", "O5_conflicts_poisoned"),
        ("smuggle_redacted_ref_via_evidence", "O6_recurring_pattern"),
        ("smuggle_redacted_ref_via_evidence", "O7_null_valid_from"),
        ("smuggle_redacted_ref_via_evidence", "O7_unpinned"),
        ("smuggle_redacted_ref_via_evidence", "O7_valid"),
        ("strip_evidence_provenance", "O1_ci_prior_attempts_squash"),
        ("strip_evidence_provenance", "O3_supersession_deterministic_only"),
        ("strip_evidence_provenance", "O3_supersession_extraction_down"),
        ("strip_evidence_provenance", "O4_prior_attempts_graph_outage"),
        ("strip_evidence_provenance", "O5_conflicts"),
        ("strip_evidence_provenance", "O5_conflicts_injected"),
        ("strip_invalidation_provenance", "O1_ci_prior_attempts"),
        ("strip_invalidation_provenance", "O1_ci_prior_attempts_squash"),
        ("strip_invalidation_provenance", "O1_ci_prior_attempts_stale"),
        ("strip_invalidation_provenance", "O2_blocking_observed"),
        ("strip_invalidation_provenance", "O3_supersession_deterministic_only"),
        ("strip_invalidation_provenance", "O3_supersession_extraction_down"),
        ("strip_invalidation_provenance", "O4_prior_attempts"),
        ("strip_invalidation_provenance", "O4_prior_attempts_after_redaction"),
        ("strip_invalidation_provenance", "O4_prior_attempts_after_revocation"),
        ("strip_invalidation_provenance", "O4_prior_attempts_graph_outage"),
        ("strip_invalidation_provenance", "O4_prior_attempts_manipulated"),
        ("strip_invalidation_provenance", "O5_conflicts"),
        ("strip_invalidation_provenance", "O5_conflicts_injected"),
        ("strip_invalidation_provenance", "O5_conflicts_poisoned"),
        ("strip_invalidation_provenance", "O6_recurring_pattern"),
        ("strip_invalidation_provenance", "O7_null_valid_from"),
        ("strip_invalidation_provenance", "O7_valid"),
        ("substitute_canonical_id", "O1_ci_prior_attempts_squash"),
        ("substitute_canonical_id", "O3_supersession_deterministic_only"),
        ("substitute_canonical_id", "O3_supersession_extraction_down"),
        ("substitute_canonical_id", "O4_prior_attempts_graph_outage"),
        ("suppress_warnings", "O1_ci_prior_attempts"),
        ("suppress_warnings", "O2_blocking_observed"),
        ("suppress_warnings", "O2_blocking_valid"),
        ("suppress_warnings", "O3_supersession"),
        ("suppress_warnings", "O4_prior_attempts"),
        ("suppress_warnings", "O4_prior_attempts_after_revocation"),
        ("suppress_warnings", "O4_prior_attempts_manipulated"),
        ("suppress_warnings", "O5_conflicts"),
        ("suppress_warnings", "O5_conflicts_injected"),
        ("suppress_warnings", "O5_conflicts_poisoned"),
        ("suppress_warnings", "O6_recurring_pattern"),
        ("suppress_warnings", "O7_null_valid_from"),
        ("suppress_warnings", "O7_valid"),
    }
)


def test_inapplicable_pairs_match_the_pinned_manifest() -> None:
    """Guard against silent coverage shrinkage in the INAPPLICABLE skip set.

    A pair moving INAPPLICABLE -> APPLIED is always fine (it is new
    coverage). A pair moving the other way, or a new pair appearing
    INAPPLICABLE, means some oracle or fault mode stopped being exercised by
    something that used to exercise it -- and with 174 pre-existing skips,
    one more is invisible unless something is pinned to compare against.
    """
    observed = frozenset(
        (fault.fault_id, oracle.oracle_id)
        for fault in FAULT_MODES
        for oracle in ALL_ORACLES
        if fault.apply(oracle, golden_response(oracle, ARM)).application
        is FaultApplication.INAPPLICABLE
    )
    newly_inapplicable = observed - _PINNED_INAPPLICABLE_PAIRS
    newly_applicable = _PINNED_INAPPLICABLE_PAIRS - observed
    assert not newly_inapplicable and not newly_applicable, (
        "the INAPPLICABLE (fault, oracle) manifest drifted -- "
        f"newly INAPPLICABLE (coverage shrank, investigate): "
        f"{sorted(newly_inapplicable)}; "
        f"newly APPLICABLE (coverage grew, update the pinned manifest): "
        f"{sorted(newly_applicable)}"
    )


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


def test_conflicts_injected_requires_untrusted_content_not_just_conflicting() -> None:
    """C14: legit facts sharing the untrusted source must carry untrusted_content.

    Regression for the finding that O5_CONFLICTS_INJECTED asserted only
    `conflicting`, so an arm that laundered injected content through the
    extractor without tagging its legitimate siblings `untrusted_content`
    passed anyway.
    """
    oracle = ORACLES_BY_ID["O5_conflicts_injected"]
    golden = golden_response(oracle, ARM)
    assert oracle.evaluate(golden).verdict is Verdict.PASS

    index, fact = next(
        (i, f) for i, f in enumerate(golden.facts) if f.subject_ref == gt.INCIDENT_503
    )
    laundered = dataclasses.replace(
        golden,
        facts=(
            *golden.facts[:index],
            dataclasses.replace(
                fact, flags=dataclasses.replace(fact.flags, untrusted_content=False)
            ),
            *golden.facts[index + 1 :],
        ),
    )
    result = oracle.evaluate(laundered)
    assert result.verdict is Verdict.FAIL, (
        "an arm that dropped untrusted_content from a fact sharing the "
        "injected source's provenance must not pass -- the conflicting flag "
        "alone is not the invariant C14 exists to catch"
    )
    assert any(
        a.assertion_id.startswith("must_include") for a in result.assertions if not a.ok
    )


def test_hide_coverage_gap_mutates_the_source_the_oracle_actually_checks() -> None:
    """Regression: the old mutator picked the response's first unavailable
    source regardless of whether any oracle assertion watched it. On a
    two-gap scenario that could silently mutate an unwatched source, leave
    the oracle's real coverage expectation untouched, and let the fault be
    scored as caught when it never touched anything the oracle checks.
    """
    oracle = ORACLES_BY_ID["O1_ci_prior_attempts_squash"]
    golden = golden_response(oracle, ARM)
    assert oracle.evaluate(golden).verdict is Verdict.PASS

    # An extra gap that sorts/iterates first but that no oracle assertion
    # names -- the old code's `gaps[0]` would grab this one instead.
    two_gaps = {
        "aaa_unwatched_source": SourceCoverage(
            source="aaa_unwatched_source", available=False, reason="unrelated"
        ),
        **golden.source_coverage,
    }
    response = dataclasses.replace(golden, source_coverage=two_gaps)

    outcome = hide_coverage_gap(oracle, response)
    assert outcome.application is FaultApplication.APPLIED
    assert outcome.response.source_coverage["work_graph_pr_commit"].available is True, (
        "the mutator must hide the gap the oracle's coverage expectation actually names"
    )
    assert (
        outcome.response.source_coverage["aaa_unwatched_source"].available is False
    ), "the unwatched gap must be left alone -- mutating it proves nothing"

    result = oracle.evaluate(outcome.response)
    assert result.verdict.is_failure, (
        "hiding the watched gap must fail the oracle even when an unrelated "
        "gap is also present in the response"
    )


def test_golden_response_scopes_by_query_subject_not_just_predicate() -> None:
    """Regression: golden_response never passed `subjects` to gt.select, so
    an arm that leaked predicate-matching material for an entity nobody
    asked about (e.g. a repo_atlas_web episode when only repo_atlas_api was
    in scope) went undetected -- the golden reference contained the same
    leak, so no assertion could tell correct from leaky.
    """
    oracle = ORACLES_BY_ID["O4_prior_attempts"]
    golden = golden_response(oracle, ARM)
    assert golden.facts, (
        "golden response must not be empty for this assertion to mean anything"
    )
    assert all(f.object_ref == gt.REPO_API for f in golden.facts), (
        "O4_prior_attempts asked only about repo_atlas_api but the golden "
        f"response included: {[str(f.object_ref) for f in golden.facts]}"
    )


def test_arm_response_leaking_out_of_subject_fact_fails_the_oracle() -> None:
    """Regression: finding 10 pinned subject scoping in the golden BUILDER,
    but nothing checked a real arm's response -- an arm serving a
    repo_atlas_web episode when only repo_atlas_api was queried passed
    O4_prior_attempts anyway, because the leak was never asserted against.

    RED evidence: with require_subject_scoped forced off (simulating the
    oracle before this fix), the exact same leaked response evaluates to
    PASS. See PR comment for the reproduction transcript.
    """
    oracle = ORACLES_BY_ID["O4_prior_attempts"]
    golden = golden_response(oracle, ARM)
    leak = TemporalFact(
        fact_id="tf_gt_ep5_web_repo",
        subject_ref=gt.EPISODE_WEB_REPO,
        predicate="touched",
        object_ref=gt.REPO_WEB,
        observed_at=gt.TRIAL_NOW,
        claim_kind=ClaimKind.OBSERVED,
        projection_version="temporal-projector.v1",
        evidence_refs=("ev1_ep_0005",),
    )
    leaked = dataclasses.replace(golden, facts=(*golden.facts, leak))

    result = oracle.evaluate(leaked)
    assert result.verdict is Verdict.FAIL, (
        "an arm leaking material for an entity outside query.subjects must "
        "fail, not pass because the leaked fact happened to match a "
        "predicate nobody excluded"
    )
    assert "subject_scoped" in result.failed_assertion_ids()

    # Control: the same mutation with subject scoping disabled (the old
    # behaviour) must still pass -- proving the new assertion, not some
    # other side effect, is what catches the leak.
    old_oracle = dataclasses.replace(oracle, require_subject_scoped=False)
    assert old_oracle.evaluate(leaked).verdict is Verdict.PASS


def test_golden_response_declares_truncation_when_a_candidate_is_dropped() -> None:
    """Regression: golden.py hardcoded truncated=False regardless of whether
    max_results actually dropped a candidate.
    """
    oracle = ORACLES_BY_ID["O4_prior_attempts_manipulated"]
    golden = golden_response(oracle, ARM)
    assert len(golden.facts) == oracle.query.max_results
    assert golden.truncated is True, (
        "golden dropped candidates to fit max_results but stamped "
        "truncated=False -- an arm doing the same thing would be reporting "
        "a lossy answer as complete"
    )


def test_golden_response_does_not_falsely_claim_truncation() -> None:
    """The other half of the truncation fix: no false positives either."""
    oracle = ORACLES_BY_ID["O4_prior_attempts"]
    golden = golden_response(oracle, ARM)
    assert len(golden.facts) < oracle.query.max_results
    assert golden.truncated is False


def test_manipulated_truncation_never_lets_the_decoy_displace_real_evidence() -> None:
    """C17, stated directly, against the REAL corpus.

    On its own this is a weak witness for the sort: in the pinned corpus
    the keyword-stuffed decoy already happens to be GROUND_TRUTH's
    last-declared fact among O4_prior_attempts_manipulated's candidates, so
    this assertion holds whether or not `_adversarial_last` actually runs.
    Kept as an end-to-end regression companion; the tests below are what
    actually exercise the sort.
    """
    oracle = ORACLES_BY_ID["O4_prior_attempts_manipulated"]
    golden = golden_response(oracle, ARM)
    assert not any(f.fact_id == "tf_gt_ep_keyword_stuffed" for f in golden.facts), (
        "the keyword-stuffed decoy must not survive the truncation budget"
    )


def test_adversarial_last_sort_reorders_regardless_of_declaration_order() -> None:
    """Direct unit test of the ordering primitive golden_response relies on.

    Regression for the verifier's finding that the end-to-end test above
    cannot fail against a deleted sort: GROUND_TRUTH happens to declare
    every planted decoy after the real evidence it decoys, so removing
    `_adversarial_last` from golden_response does not change
    O4_prior_attempts_manipulated's output at all. This test builds its OWN
    input order (decoy first) so the sort's effect is directly observable,
    independent of GROUND_TRUTH's declaration order.
    """
    decoy = gt.GROUND_TRUTH_BY_KEY["gt_ep_keyword_stuffed"]
    real = gt.GROUND_TRUTH_BY_KEY["gt_ep1_touched"]
    assert decoy.is_adversarial
    assert not real.is_adversarial

    ordered = _adversarial_last([decoy, real])
    assert [f.fact_key for f in ordered] == [real.fact_key, decoy.fact_key], (
        "adversarial material must be reordered after legitimate evidence "
        "even when GROUND_TRUTH-equivalent input declares it first"
    )


def test_golden_response_still_orders_correctly_when_declaration_order_is_reversed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end companion to the direct unit test above.

    Monkeypatches GROUND_TRUTH so the keyword-stuffed decoy is declared
    BEFORE the three real attempts -- the opposite of the pinned corpus.
    Without `_adversarial_last` actually wired into golden_response, the
    decoy would now occupy one of the three truncation slots and a real
    attempt would be dropped; this fails loudly if that wiring regresses.
    """
    decoy = gt.GROUND_TRUTH_BY_KEY["gt_ep_keyword_stuffed"]
    others = tuple(f for f in gt.GROUND_TRUTH if f.fact_key != decoy.fact_key)
    reversed_ground_truth = (decoy, *others)
    assert reversed_ground_truth.index(decoy) < reversed_ground_truth.index(
        gt.GROUND_TRUTH_BY_KEY["gt_ep1_touched"]
    )

    monkeypatch.setattr(gt, "GROUND_TRUTH", reversed_ground_truth)

    oracle = ORACLES_BY_ID["O4_prior_attempts_manipulated"]
    golden = golden_response(oracle, ARM)
    assert len(golden.facts) == 3
    assert not any(f.fact_id == "tf_gt_ep_keyword_stuffed" for f in golden.facts), (
        "with the decoy declared FIRST, only a real sort (not declaration "
        "order) keeps it out of the truncated response"
    )
    assert {f.fact_id for f in golden.facts} == {
        "tf_gt_ep1_touched",
        "tf_gt_ep2_touched",
        "tf_gt_ep3_touched",
    }
