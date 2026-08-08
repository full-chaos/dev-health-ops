"""Guards on the baseline-versus-arms report shape.

Amended §14 restructured the native work as pre-trial increments feeding a
**native baseline** — not as a competing entrant. These tests hold that shape
in place, because a flat four-way league table would let a candidate "win" a
class by placing above one baseline component while losing to the baseline as
a whole.
"""

from __future__ import annotations

import pytest

from ..corpus.oracles import ALL_ORACLES
from ..harness.contracts import ALL_QUESTION_CLASSES, ArmResponse, QuestionClass
from ..harness.oracle import Oracle, Verdict
from ..harness.runner import (
    UNRECORDED_DEPENDENCY,
    ArmRegistry,
    ArmRole,
    ComparisonReport,
    ControlStatus,
    DependencyState,
    compare,
    compose_baseline,
    run_trial,
)
from .golden import golden_response

_PERFECT = "perfect"


def _perfect_arm(oracle: Oracle) -> ArmResponse:
    return golden_response(oracle, _PERFECT)


def _dead_arm(name: str):
    def _arm(_: Oracle) -> ArmResponse:
        return ArmResponse.not_run(name, "stack not brought up")

    return _arm


def _blind_arm(oracle: Oracle) -> ArmResponse:
    """Answers nothing at all, but answers it successfully."""
    return ArmResponse(arm="blind", outcome=oracle.expect_outcome, query=oracle.query)


def _registry(**arms) -> ArmRegistry:
    registry = ArmRegistry()
    for name, (arm, role) in arms.items():
        registry.register(name, arm, role)
    return registry


def test_baseline_passes_when_any_component_passes() -> None:
    """The baseline is what the product can already do, by any of its parts."""
    strong = run_trial(ALL_ORACLES, "native", _perfect_arm)
    weak = run_trial(ALL_ORACLES, "readback", _blind_arm)
    baseline = compose_baseline([weak, strong])
    assert all(r.verdict is Verdict.PASS for r in baseline.results)


def test_fully_measured_baseline_that_cannot_answer_records_a_real_failure() -> None:
    """Control for the test below: with every component run, FAIL is FAIL.

    Without this pair, "the baseline was NOT MEASURED" could be produced by a
    fold that never emits FAIL at all -- which would make the baseline
    unbeatable and every candidate delta meaningless.
    """
    blind = run_trial(ALL_ORACLES, "readback", _blind_arm)
    baseline = compose_baseline([blind])
    verdicts = {r.verdict for r in baseline.results}
    assert Verdict.FAIL in verdicts, (
        "a fully-measured baseline that answers nothing must record failures"
    )
    assert Verdict.NOT_MEASURED not in verdicts


def test_unmeasured_component_never_becomes_a_baseline_failure() -> None:
    """The rule that stops a candidate beating a baseline nobody ran.

    Same blind component as the control above, plus one component that was
    never run. Every row that the control recorded as FAIL must now degrade
    to NOT MEASURED: we know readback cannot answer, we do NOT know whether
    native could, so "the product already does this" is genuinely unknown.
    Recording FAIL would assert the product cannot do something nobody asked
    it to do, and the candidate would be credited with a win it never earned.
    """
    blind = run_trial(ALL_ORACLES, "readback", _blind_arm)
    absent = run_trial(ALL_ORACLES, "native", _dead_arm("native"))
    baseline = compose_baseline([absent, blind])
    control = compose_baseline([blind])

    degraded = 0
    for folded, control_row, absent_row in zip(
        baseline.results, control.results, absent.results, strict=True
    ):
        assert absent_row.verdict is Verdict.NOT_MEASURED
        if control_row.verdict is Verdict.PASS:
            assert folded.verdict is Verdict.PASS, (
                "an answer one component actually gave must survive another "
                "component being absent"
            )
        else:
            assert folded.verdict is Verdict.NOT_MEASURED
            degraded += 1

    assert degraded, (
        "no row degraded, so this test would pass against a fold that ignores "
        "unmeasured components entirely"
    )


def test_compare_requires_at_least_one_baseline_component() -> None:
    registry = _registry(graphiti=(_perfect_arm, ArmRole.CANDIDATE_ARM))
    with pytest.raises(ValueError, match="no baseline components"):
        compare(ALL_ORACLES, registry)


def test_class_b_is_not_comparable_until_chaos_3563_state_is_recorded() -> None:
    """An unrecorded dependency must render NOT COMPARABLE, not as a number.

    "Native scored 0 on class (b)" means something entirely different before
    and after CHAOS-3563 lands declared-state retention. A report that does
    not say which one it measured is not a result.
    """
    registry = _registry(
        native=(_perfect_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_perfect_arm, ArmRole.CANDIDATE_ARM),
    )
    (report,) = compare(ALL_ORACLES, registry)

    by_class = {c.question_class: c for c in report.by_class()}
    class_b = by_class[QuestionClass.NEEDS_DECLARED_STATE_HISTORY]
    assert not class_b.is_comparable
    assert "CHAOS-3563" in class_b.render()
    assert "NOT COMPARABLE" in class_b.render()

    # Every other class is unaffected by that dependency.
    assert by_class[QuestionClass.NATIVE_ANSWERABLE].is_comparable


def test_recording_the_dependency_makes_class_b_comparable() -> None:
    registry = _registry(
        native=(_perfect_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_perfect_arm, ArmRole.CANDIDATE_ARM),
    )
    (report,) = compare(
        ALL_ORACLES,
        registry,
        dependencies={
            QuestionClass.NEEDS_DECLARED_STATE_HISTORY: DependencyState(
                issue="CHAOS-3563",
                state="not landed -- baseline measured WITHOUT retention",
            )
        },
    )
    by_class = {c.question_class: c for c in report.by_class()}
    assert by_class[QuestionClass.NEEDS_DECLARED_STATE_HISTORY].is_comparable


def test_wrong_dependency_issue_is_rejected() -> None:
    registry = _registry(
        native=(_perfect_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_perfect_arm, ArmRole.CANDIDATE_ARM),
    )
    with pytest.raises(ValueError, match="CHAOS-3563"):
        compare(
            ALL_ORACLES,
            registry,
            dependencies={
                QuestionClass.NEEDS_DECLARED_STATE_HISTORY: DependencyState(
                    issue="CHAOS-9999", state="wrong issue"
                )
            },
        )


def test_unrecorded_dependency_is_the_default_direction() -> None:
    """Safe direction: silence means unrecorded, not 'fine'."""
    assert not UNRECORDED_DEPENDENCY.recorded
    assert UNRECORDED_DEPENDENCY.issue == "CHAOS-3563"


def test_unmeasured_arm_makes_its_class_not_comparable() -> None:
    registry = _registry(
        native=(_perfect_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_dead_arm("graphiti"), ArmRole.CANDIDATE_ARM),
    )
    (report,) = compare(ALL_ORACLES, registry)
    for comparison in report.by_class():
        assert not comparison.is_comparable
        assert "NOT MEASURED" in comparison.render()


def test_class_a_control_failure_is_shouted_not_buried() -> None:
    """PRD §15.2: if the baseline loses class (a), the harness is suspect.

    A candidate that outscores the baseline on natively-answerable questions
    has almost certainly been handed an advantage the baseline did not get.
    The report must say so loudly rather than let a reader page past a
    positive delta.
    """
    registry = _registry(
        native=(_blind_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_perfect_arm, ArmRole.CANDIDATE_ARM),
    )
    (report,) = compare(
        ALL_ORACLES,
        registry,
        dependencies={
            QuestionClass.NEEDS_DECLARED_STATE_HISTORY: DependencyState(
                issue="CHAOS-3563", state="recorded for this test"
            )
        },
    )
    assert not report.native_control_holds()
    assert "class (a) control did NOT hold" in report.render()
    # Authoring-round addition: the DISTINCT-status method must agree with
    # the rendered banner, not just the collapsed bool.
    assert report.native_control_status() is ControlStatus.LOST


def test_class_a_control_holds_when_baseline_ties() -> None:
    registry = _registry(
        native=(_perfect_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_perfect_arm, ArmRole.CANDIDATE_ARM),
    )
    (report,) = compare(
        ALL_ORACLES,
        registry,
        dependencies={
            QuestionClass.NEEDS_DECLARED_STATE_HISTORY: DependencyState(
                issue="CHAOS-3563", state="recorded for this test"
            )
        },
    )
    assert report.native_control_holds()
    assert report.native_control_status() is ControlStatus.HELD
    rendered = report.render()
    assert "did NOT hold" not in rendered
    assert "NOT MEASURED" not in rendered


def test_class_a_control_not_measured_renders_distinctly_from_lost() -> None:
    """Authoring-round fix, pinned directly: a candidate that has not been
    measured against class (a) at all must render as NOT MEASURED, never
    with the "did NOT hold" banner a genuine loss gets -- the two are
    different findings (a scope gap versus a harness-suspect regression)
    and a reader must be able to tell them apart from the report text
    alone, not have to cross-reference native_control_holds()'s bool.
    """
    registry = _registry(
        native=(_perfect_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_dead_arm("graphiti"), ArmRole.CANDIDATE_ARM),
    )
    (report,) = compare(
        ALL_ORACLES,
        registry,
        dependencies={
            QuestionClass.NEEDS_DECLARED_STATE_HISTORY: DependencyState(
                issue="CHAOS-3563", state="recorded for this test"
            )
        },
    )
    assert not report.native_control_holds()
    assert report.native_control_status() is ControlStatus.NOT_MEASURED
    rendered = report.render()
    assert "class (a) control NOT MEASURED" in rendered
    assert "did NOT hold" not in rendered


def test_report_never_emits_a_single_headline_number() -> None:
    """The aggregate that §15.2 forbids must not be reachable from the report.

    With this question set weighted (a)x1 (b)x1 (c)x5, one number would
    flatter any extraction-capable candidate regardless of merit.
    """
    assert not hasattr(ComparisonReport, "score")
    assert not hasattr(ComparisonReport, "total")
    registry = _registry(
        native=(_perfect_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_perfect_arm, ArmRole.CANDIDATE_ARM),
    )
    (report,) = compare(ALL_ORACLES, registry)
    rendered = report.render()
    for klass in ALL_QUESTION_CLASSES:
        assert f"class {klass.value}:" in rendered


def test_both_baseline_components_are_registerable_as_baseline() -> None:
    """Episode readback is a baseline component, not a candidate arm.

    Amended §14: the native increments and plain episode readback are both
    part of what the graph must beat. Registering readback as a candidate
    would let Graphiti be scored against native alone.
    """
    registry = _registry(
        native=(_perfect_arm, ArmRole.BASELINE_COMPONENT),
        episode_readback=(_blind_arm, ArmRole.BASELINE_COMPONENT),
        graphiti=(_perfect_arm, ArmRole.CANDIDATE_ARM),
        direct_store=(_perfect_arm, ArmRole.CANDIDATE_ARM),
    )
    assert set(registry.names_with_role(ArmRole.BASELINE_COMPONENT)) == {
        "native",
        "episode_readback",
    }
    reports = compare(ALL_ORACLES, registry)
    assert len(reports) == 2, "one comparison per candidate arm"
    assert {r.arm.arm for r in reports} == {"graphiti", "direct_store"}
    assert all(r.baseline.arm == "baseline" for r in reports)


def test_class_score_denominator_excludes_unmeasured_oracles() -> None:
    """[codex M1] `0/15` where 11 were never run reads as a measured
    15-case score. The denominator must be what was actually measured, with
    the unmeasured count reported beside it rather than folded in.
    """
    from ..harness.contracts import QuestionClass
    from ..harness.runner import ClassScore

    score = ClassScore(
        question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
        passed=0,
        failed=4,
        not_measured=11,
    )
    assert score.measured == 4
    rendered = score.render()
    assert "0/4 measured" in rendered
    assert "0/15" not in rendered, "the unmeasured rows are back in the denominator"
    assert "11 NOT MEASURED" in rendered


def test_not_comparable_rows_render_no_numeric_delta() -> None:
    """[codex M1] A signed delta on a row the report is simultaneously
    declaring NOT COMPARABLE invites the exact comparison being disclaimed;
    readers quote the number and drop the caveat.
    """
    from ..harness.contracts import QuestionClass
    from ..harness.runner import ClassComparison, ClassScore

    klass = QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION
    comparison = ClassComparison(
        question_class=klass,
        baseline=ClassScore(klass, passed=1, failed=3, not_measured=11),
        arm=ClassScore(klass, passed=0, failed=4, not_measured=11),
        dependency=None,
    )
    assert not comparison.is_comparable
    rendered = comparison.render()
    assert "delta" not in rendered
    assert "NOT COMPARABLE" in rendered
