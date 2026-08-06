"""Unit coverage for ``status_completion_copy``'s shared "is this completion
assessment trustworthy" predicate (CHAOS-3297 s2 round 9's own module).

CHAOS-3409 codex adversarial review (HIGH, confirmed): before this fix,
``is_completion_assessment_untrustworthy`` treated EVERY withheld
(``required_child_total is None``) denominator identically -- a genuine
source truncation (real required items exist, not all were fetched) and
CHAOS-3408's structural non-applicability (ORGANIZATION/TEAM scope has no
required-child concept at all) both satisfied the SAME ``required_child_total
is None`` clause, so both were "untrustworthy": both forced the model-facing
``INCOMPLETE_DENOMINATOR_DISCLOSURE`` obligation (``answer_validator.py``),
both drove ``render_verdict_summary``'s truncation disclosure, and CHAOS-3409's
own render tests never caught it because they called ``render_verdict_summary``
directly, with a hand-supplied ``denominator_withheld``, never through this
shared predicate the real orchestrator wiring actually calls.
"""

from __future__ import annotations

from dev_health_ops.api.dev.contracts import DevActualCompletion
from dev_health_ops.api.dev.status_completion_copy import (
    is_completion_assessment_untrustworthy,
)


def _actual(
    *,
    required_child_total: int | None,
    reason_codes: tuple[str, ...] = (),
    required_children_not_applicable: bool = False,
) -> DevActualCompletion:
    return DevActualCompletion(
        state="ready",
        rule_id="actual-completion",
        rule_version="actual-completion.v4",
        reason_codes=list(reason_codes),
        required_children=[],
        blockers=[],
        required_child_total=required_child_total,
        required_child_complete=required_child_total,
        display_truncated=False,
        conflicts=[],
        evidence_ref_ids=[],
        required_children_not_applicable=required_children_not_applicable,
    )


def test_a_real_denominator_is_trustworthy() -> None:
    assert is_completion_assessment_untrustworthy(_actual(required_child_total=3)) is (
        False
    )


def test_a_real_zero_denominator_is_trustworthy() -> None:
    assert (
        is_completion_assessment_untrustworthy(_actual(required_child_total=0)) is False
    )


def test_a_genuine_source_truncation_is_untrustworthy() -> None:
    actual = _actual(
        required_child_total=None,
        reason_codes=("assessment_source_limit_reached",),
        required_children_not_applicable=False,
    )
    assert is_completion_assessment_untrustworthy(actual) is True


def test_structural_non_applicability_is_trustworthy_not_untrustworthy() -> None:
    """The load-bearing distinction: CHAOS-3408's ORGANIZATION/TEAM
    structural absence is NOT a verification failure -- the state/reason
    codes it carries are fully real, only the required-child concept does
    not apply. Treating it as "untrustworthy" is exactly what forced the
    model to disclose a truncation that never happened.
    """

    actual = _actual(
        required_child_total=None,
        reason_codes=(),
        required_children_not_applicable=True,
    )
    assert is_completion_assessment_untrustworthy(actual) is False


def test_a_withheld_denominator_with_no_signal_at_all_stays_untrustworthy() -> None:
    """Defensive: ``required_child_total is None`` with NEITHER the
    truncation reason code NOR the structural flag set must still fail
    closed to "untrustworthy" -- an unrecognized withholding is exactly
    the kind of "unknown" this predicate exists to catch, never silently
    treated as safe."""

    actual = _actual(
        required_child_total=None,
        reason_codes=(),
        required_children_not_applicable=False,
    )
    assert is_completion_assessment_untrustworthy(actual) is True
