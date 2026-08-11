"""What happened to one (case, arm) pair, as a closed set of outcomes.

CHAOS-3619 requires FAIL, NOT_RUN, NOT_COMPARABLE and expected-limitation to
be distinguishable in the raw records and in the report. This module is that
distinction, and the reason it is a closed enum rather than a string is that
every one of these means something different to a reader deciding whether
graph assistance is worth a dependency:

* ``SCORED`` -- the arm emitted a packet and the frozen oracles scored it.
  The only disposition whose dimension results mean anything.
* ``ARM_DECLARED_GAP`` -- the arm ran and said it cannot express this run.
  A **result**, not a failure: the native baseline reports several kinds of
  run as unprojectable by design, and how often it must do so is one of the
  numbers the comparison turns on.
* ``ARM_REFUSED`` -- the arm raised a named refusal (an unsupported
  comparison shape, an incomparable cohort). Distinct from a declared gap
  because a refusal names a capability boundary the arm knows it has, and
  distinct from a fault because nothing went wrong.
* ``ARM_FAULT`` -- the arm raised something it does not model. A defect,
  and it must never be counted as a capability limit.
* ``NOT_RUN_TIMEOUT`` -- the per-case wall clock expired. Never retried.
* ``NOT_RUN_PRECONDITION`` -- the leg could not start (no store, no extra,
  no signing secret). The measurement did not happen and says so.
* ``NOT_COMPARABLE`` -- the arms cannot be compared on this case for a
  reason that is a property of the trial rather than of either arm, e.g.
  CHAOS-3569's historical/as-of rows with no native edge history.
* ``EXPECTED_LIMITATION`` -- a named, recorded debt makes this cell
  unmeasurable today. Carries the debt's identifier so it can never read as
  a pass or as a mystery.

The rule that makes the set worth having: **only ``SCORED`` may carry
dimension results.** Every other disposition renders as itself in the
report, never as a zero and never as a blank, because a zero is a
measurement and a blank is an oversight.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "MEASURED_DISPOSITIONS",
    "NOT_RUN_DISPOSITIONS",
    "CaseDisposition",
    "is_measured",
]


class CaseDisposition(StrEnum):
    SCORED = "scored"
    ARM_DECLARED_GAP = "arm_declared_gap"
    ARM_REFUSED = "arm_refused"
    ARM_FAULT = "arm_fault"
    NOT_RUN_TIMEOUT = "not_run_timeout"
    NOT_RUN_PRECONDITION = "not_run_precondition"
    NOT_COMPARABLE = "not_comparable"
    EXPECTED_LIMITATION = "expected_limitation"


#: The only disposition whose dimension results are meaningful.
#:
#: A frozenset of one, spelled as a set rather than an ``is`` comparison, so
#: that adding a second measured disposition later is one edit here instead
#: of a search for every place that compared against the single member.
MEASURED_DISPOSITIONS: frozenset[CaseDisposition] = frozenset({CaseDisposition.SCORED})

#: Dispositions that mean no measurement happened at all. These must be
#: reported loudly rather than folded into a denominator: a run that timed
#: out is not a run that scored zero, and averaging them together is how a
#: harness reports coverage it never had.
NOT_RUN_DISPOSITIONS: frozenset[CaseDisposition] = frozenset(
    {
        CaseDisposition.NOT_RUN_TIMEOUT,
        CaseDisposition.NOT_RUN_PRECONDITION,
        CaseDisposition.EXPECTED_LIMITATION,
        CaseDisposition.NOT_COMPARABLE,
    }
)


def is_measured(disposition: CaseDisposition) -> bool:
    return disposition in MEASURED_DISPOSITIONS
