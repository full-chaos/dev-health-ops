"""Bring-up step 1: baseline-component adapters against the pinned corpus.

harness-design.md §7 lists step 1 ("Arm adapters against fixture data") as
needing no environment slot and no landed dependency, distinct from step 5
("Arms N and E measured"), which needs CHAOS-3563/3564/3565 landed. This
suite is step 1's proof: the two baseline-component adapters
(:mod:`harness.arms.native`, :mod:`harness.arms.episode_readback`) run
against the real corpus, register correctly as
``ArmRole.BASELINE_COMPONENT``, fold through ``compose_baseline``, and
render per-class through both ``TrialReport`` and ``ComparisonReport`` --
with no live stack, no LLM spend, and no other lane's code.

These are not "does it crash" tests. Every assertion below is grounded in
either the corpus's own construction (what native/episode readback can
genuinely answer, per docs/baseline-inventory.md) or the harness's own
documented composition rules -- because a baseline-component suite that only
proves the adapters ran, without proving they answered the RIGHT questions
correctly and the WRONG ones honestly, would read as coverage of nothing.
"""

from __future__ import annotations

from ..corpus import ground_truth as gt
from ..corpus.oracles import ALL_ORACLES, ORACLES_BY_ID
from ..harness.arms import episode_readback, native
from ..harness.contracts import ALL_QUESTION_CLASSES, QuestionClass
from ..harness.oracle import Verdict
from ..harness.runner import ArmRegistry, ArmRole, compare, compose_baseline, run_trial

# No dependencies dict is supplied to compare() anywhere in this suite,
# deliberately: CHAOS-3563's branch state is obtained through the
# orchestrator at measurement time, not fabricated here. compare() defaults
# every class-(b) row to UNRECORDED_DEPENDENCY on its own, which is the
# honest, current truth -- CHAOS-3563 is not merged anywhere yet.


def _registry() -> ArmRegistry:
    registry = ArmRegistry()
    registry.register("native", native.answer, ArmRole.BASELINE_COMPONENT)
    registry.register(
        "episode_readback", episode_readback.answer, ArmRole.BASELINE_COMPONENT
    )
    # No candidate arm exists yet (Graphiti/direct-store follow separate
    # review, per README's "What is deliberately not here"). Registered
    # unavailable rather than omitted, so compare() has something to render
    # a ComparisonReport against and the class-(a)/(b) NOT COMPARABLE paths
    # are exercised for real, not just unit-tested in isolation.
    registry.register_unavailable(
        "candidate_placeholder", ArmRole.CANDIDATE_ARM, "no_candidate_arm_built_yet"
    )
    return registry


# --------------------------------------------------------------------------
# Registration and role shape
# --------------------------------------------------------------------------


def test_both_components_register_as_baseline_not_candidate() -> None:
    registry = _registry()
    assert set(registry.names_with_role(ArmRole.BASELINE_COMPONENT)) == {
        "native",
        "episode_readback",
    }
    assert registry.names_with_role(ArmRole.CANDIDATE_ARM) == ("candidate_placeholder",)


# --------------------------------------------------------------------------
# Grounded, per-oracle expectations -- what each component can genuinely
# answer, per docs/baseline-inventory.md.
# --------------------------------------------------------------------------


def test_native_answers_the_class_a_control_it_owns() -> None:
    """PRD §15.2's class (a) control: native has real valid-time intervals
    on operational_service_repository_mappings and a real interval filter.
    If this fails, the harness is suspect -- see O7_VALID's own rationale.
    """
    oracle = ORACLES_BY_ID["O7_valid"]
    result = oracle.evaluate(native.answer(oracle))
    assert result.verdict is Verdict.PASS, (
        f"native failed its own class (a) control: "
        f"{[(a.assertion_id, a.detail) for a in result.assertions if not a.ok]}"
    )


def test_native_reproduces_the_documented_null_valid_from_defect() -> None:
    """baseline-inventory.md §4: valid_from is Nullable and NULL <= as_of is
    false in ClickHouse, so a null-started row is silently dropped on every
    axis. This is an ops finding, not a graph finding -- native is EXPECTED
    to fail this oracle, and O7_null_valid_from stays outside the class-(a)
    control specifically so no candidate arm claims credit for it.
    """
    oracle = ORACLES_BY_ID["O7_null_valid_from"]
    result = oracle.evaluate(native.answer(oracle))
    assert result.verdict is Verdict.FAIL
    assert "must_include:open-started interval holds at any as_of" in (
        result.failed_assertion_ids()
    )


def test_native_answers_blocker_questions_with_present_state_not_history() -> None:
    """corpus case C03's own named defect: "an arm that reports the current
    blocker for a past as-of date". work_graph_edges has no valid_to
    (baseline-inventory.md §5), so native's real `_BLOCKERS_SQL` cannot
    filter historically at all -- it returns whatever holds NOW regardless
    of the as-of asked. Reproduced here as real content, not an invented
    empty gap: as of TRIAL_NOW only the backfilled ATL-105 blocker is still
    open (ATL-101's closed 07-18), so that is what native's answer to BOTH
    the valid-time and observed-time oracles actually contains.
    """
    valid_oracle = ORACLES_BY_ID["O2_blocking_valid"]
    observed_oracle = ORACLES_BY_ID["O2_blocking_observed"]

    valid_response = native.answer(valid_oracle)
    observed_response = native.answer(observed_oracle)

    assert {f.subject_ref for f in valid_response.facts} == {gt.ISSUE_105}
    assert {f.subject_ref for f in observed_response.facts} == {gt.ISSUE_105}
    assert "no_relationship_valid_time:work_graph_edges_no_valid_to" in (
        valid_response.degraded_reasons
    )

    # Present-state content is wrong for BOTH historical oracles -- neither
    # can be satisfied by the same present-state answer, which is exactly
    # C19's point about the axis pair generalised to an arm with no history
    # at all.
    assert valid_oracle.evaluate(valid_response).verdict is Verdict.FAIL
    assert observed_oracle.evaluate(observed_response).verdict is Verdict.FAIL


def test_episode_readback_answers_prior_attempts_that_native_cannot() -> None:
    """The measurement amended §14 exists to make: EpisodeArtifacts is
    already structured, so plain readback answers O4_prior_attempts outright
    -- native cannot (no episode read path at all). If a future candidate
    arm's only advantage over the baseline on class (c) turns out to be this
    same question, the graph's margin here is zero.
    """
    oracle = ORACLES_BY_ID["O4_prior_attempts"]
    readback_result = oracle.evaluate(episode_readback.answer(oracle))
    native_result = oracle.evaluate(native.answer(oracle))
    assert readback_result.verdict is Verdict.PASS, (
        f"episode_readback failed a plain prior-attempts question it should "
        f"own outright: "
        f"{[(a.assertion_id, a.detail) for a in readback_result.assertions if not a.ok]}"
    )
    assert native_result.verdict is Verdict.FAIL


def test_episode_readback_has_no_ranking_sophistication_against_the_decoy() -> None:
    """C17's premise made concrete: plain readback has no relevance ranking,
    only recency. The keyword-stuffed decoy (observed 07-11) is more recent
    than two of the three real attempts (succeeded 06-18, failed 06-25), so
    under the tight 3-result budget it displaces them; only the abandoned
    attempt (07-08, more recent than the decoy... no, LESS recent) --
    concretely: decoy, abandoned, sole-support survive; succeeded and failed
    do not. This is the exact value proposition a retrieval-manipulation-
    resistant candidate arm would need to beat.
    """
    oracle = ORACLES_BY_ID["O4_prior_attempts_manipulated"]
    response = episode_readback.answer(oracle)
    result = oracle.evaluate(response)
    assert result.verdict is Verdict.FAIL
    failed = set(result.failed_assertion_ids())
    assert "must_include:succeeded attempt survives the decoy" in failed
    assert "must_include:failed attempt survives the decoy" in failed
    assert "must_include:abandoned attempt survives the decoy" not in failed
    assert any(
        f.fact_id == "tf_episode_readback_gt_ep_keyword_stuffed" for f in response.facts
    )


def test_neither_component_extracts_or_associates() -> None:
    """Class (c) questions that need extraction (Q3, Q5, Q6) or an
    association neither component's data model carries at all (Q1's
    signature-to-repo link) must come back empty and degraded, never wrong.
    """
    for oracle_id in (
        "O1_ci_prior_attempts",
        "O3_supersession",
        "O5_conflicts",
        "O6_recurring_pattern",
    ):
        oracle = ORACLES_BY_ID[oracle_id]
        for arm in (native, episode_readback):
            response = arm.answer(oracle)
            assert response.facts == (), (
                f"{arm.ARM_NAME} invented content for {oracle_id}, which "
                "neither component's data model supports"
            )
            assert response.degraded_reasons, (
                f"{arm.ARM_NAME} returned empty for {oracle_id} without "
                "declaring why -- silent emptiness is the C16 failure mode"
            )


# --------------------------------------------------------------------------
# compose_baseline() folds the two components correctly
# --------------------------------------------------------------------------


def test_baseline_passes_class_a_via_native_even_though_readback_cannot() -> None:
    native_report = run_trial(ALL_ORACLES, "native", native.answer)
    readback_report = run_trial(
        ALL_ORACLES, "episode_readback", episode_readback.answer
    )
    baseline = compose_baseline([native_report, readback_report])

    o7_valid = next(r for r in baseline.results if r.oracle_id == "O7_valid")
    assert o7_valid.verdict is Verdict.PASS, (
        "the baseline must pass wherever ANY component passes -- native "
        "alone answers O7_valid correctly"
    )


def test_baseline_passes_class_c_via_readback_even_though_native_cannot() -> None:
    native_report = run_trial(ALL_ORACLES, "native", native.answer)
    readback_report = run_trial(
        ALL_ORACLES, "episode_readback", episode_readback.answer
    )
    baseline = compose_baseline([native_report, readback_report])

    o4 = next(r for r in baseline.results if r.oracle_id == "O4_prior_attempts")
    assert o4.verdict is Verdict.PASS, (
        "the baseline must pass wherever ANY component passes -- episode "
        "readback alone answers O4_prior_attempts correctly"
    )


def test_baseline_fails_where_both_components_genuinely_cannot_answer() -> None:
    native_report = run_trial(ALL_ORACLES, "native", native.answer)
    readback_report = run_trial(
        ALL_ORACLES, "episode_readback", episode_readback.answer
    )
    baseline = compose_baseline([native_report, readback_report])

    o1 = next(r for r in baseline.results if r.oracle_id == "O1_ci_prior_attempts")
    assert o1.verdict is Verdict.FAIL, (
        "a genuinely-measured baseline that cannot answer must record a "
        "real FAIL, not something that reads as untested"
    )


# --------------------------------------------------------------------------
# Per-class rendering -- the actual "confirm compare() renders per-class"
# ask. Numbers pinned to the observed matrix so a regression in either
# adapter is caught by name, not just "still runs".
# --------------------------------------------------------------------------


def test_trial_report_renders_per_class_for_each_component() -> None:
    native_report = run_trial(ALL_ORACLES, "native", native.answer)
    readback_report = run_trial(
        ALL_ORACLES, "episode_readback", episode_readback.answer
    )

    native_classes = native_report.by_class()
    assert native_classes[QuestionClass.NATIVE_ANSWERABLE].passed == 1  # O7_valid
    assert native_classes[QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION].passed == 0

    readback_classes = readback_report.by_class()
    assert readback_classes[QuestionClass.NATIVE_ANSWERABLE].passed == 0
    assert (
        readback_classes[QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION].passed == 1
    )  # O4_prior_attempts

    rendered = native_report.render()
    for klass in ALL_QUESTION_CLASSES:
        assert f"class {klass.value}:" in rendered


def test_comparison_report_renders_per_class_against_the_composed_baseline() -> None:
    registry = _registry()
    (report,) = compare(ALL_ORACLES, registry)

    rendered = report.render()
    for klass in ALL_QUESTION_CLASSES:
        assert f"class {klass.value}:" in rendered

    by_class = {c.question_class: c for c in report.by_class()}
    # The placeholder candidate is fully unmeasured, so every class must
    # render NOT COMPARABLE -- proving the harness's own honesty guard
    # fires for a real (not synthetic) unmeasured arm, not just the
    # hand-built fixtures test_comparison_report.py uses.
    for klass in ALL_QUESTION_CLASSES:
        assert not by_class[klass].is_comparable
        assert "NOT COMPARABLE" in by_class[klass].render()
    assert not report.native_control_holds()
    assert "class (a) control did NOT hold" in rendered


def test_class_b_stays_not_comparable_until_chaos_3563_state_is_supplied() -> None:
    """Successor must-know from the handoff: CHAOS-3563 is not merged
    anywhere yet. Confirmed here against the real baseline components, not
    just the synthetic fixtures in test_comparison_report.py.
    """
    registry = _registry()
    (report,) = compare(ALL_ORACLES, registry)
    by_class = {c.question_class: c for c in report.by_class()}
    class_b = by_class[QuestionClass.NEEDS_DECLARED_STATE_HISTORY]
    assert not class_b.is_comparable
    assert "CHAOS-3563" in class_b.render()
