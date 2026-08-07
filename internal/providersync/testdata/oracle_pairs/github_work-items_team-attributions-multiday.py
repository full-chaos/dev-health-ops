"""Live Python oracle for team attributions over a MULTI-DAY window.

This pair exists to pin CHAOS-3494, not to re-prove single-day attribution
(``github/work-items/team-attributions`` already does that).

``compute_work_item_team_attributions`` takes no ``day`` argument, yet
``job_work_items.py:1260`` calls it INSIDE the caller's ``for d in days`` loop
(:1238). A multi-day backfill therefore recomputes and re-emits byte-identical
attribution rows once per day -- pure write amplification. Per D16 the Go port
mirrors that bug-for-bug rather than repairing it, so the multiplicity itself
is the compared property: an n-day window must produce exactly n copies of the
single-day result, in day order, on BOTH sides.

The loop here mirrors the producer's structure: the resolver cascade and
attribution context are built ONCE (``DerivedCase.__init__``, matching
job_work_items.py:1216-1236 which builds them before the loop), and only the
compute call repeats. Rebuilding the context per day would be a different
program from the one in production, and would hide the very defect this pins.

WHAT THIS PAIR DOES AND DOES NOT PROVE. The loop is authored here, not read out
of the producer, so the row comparison ALONE would survive the likeliest fix to
CHAOS-3494 -- hoisting the call out of the day loop changes the producer without
changing this file, and both sides would still be told to loop n times. The
defect's continued existence is therefore asserted separately and at the source
level by ``_assert_producer_still_calls_inside_the_day_loop``. That assertion,
not the row comparison, is what fails when CHAOS-3494 is fixed.
"""

from __future__ import annotations

from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_item_derived_helpers import (
    REPO_ROOT,
    SCHEMA_SOURCE,
    DerivedCase,
    columns,
)

_RECORD = "WorkItemTeamAttributionRecord"

_JOB_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/job_work_items.py"
_CALL = "compute_work_item_team_attributions("
_LOOP = "for d in days:"


def _assert_producer_still_calls_inside_the_day_loop() -> int:
    """Assert the DEFECT still exists in the producer, and return its day count.

    This pair's loop is authored HERE, not read out of the producer, so on its
    own it would keep passing after CHAOS-3494 was fixed: hoisting the call out
    of ``for d in days:`` changes the producer without changing this file, and
    both sides would still agree on n copies because both sides would still be
    told to loop n times.

    So the mirrored defect is asserted at the SOURCE level. If someone hoists
    the call, this raises and the pair fails -- which is the intended signal to
    fix Python, then update the Go mirror and this case together under
    differential proof.
    """
    lines = _JOB_SOURCE.read_text().splitlines()
    loop_indices = [index for index, line in enumerate(lines) if line.strip() == _LOOP]
    if len(loop_indices) != 1:
        raise AssertionError(
            f"expected exactly one {_LOOP!r} in {_JOB_SOURCE.name}, "
            f"found {len(loop_indices)} -- the anchor this assertion rests on moved"
        )
    loop_index = loop_indices[0]
    loop_indent = len(lines[loop_index]) - len(lines[loop_index].lstrip())

    # Walk the loop body: everything indented deeper than the `for`, stopping at
    # the first non-blank line that dedents back to or past it.
    for index in range(loop_index + 1, len(lines)):
        line = lines[index]
        if not line.strip():
            continue
        if len(line) - len(line.lstrip()) <= loop_indent:
            break
        if _CALL in line:
            return loop_index + 1
    raise AssertionError(
        f"{_CALL!r} is no longer inside the {_LOOP!r} block of "
        f"{_JOB_SOURCE.name}. CHAOS-3494 appears to be FIXED. This pair pins the "
        "defect, so fix the Go mirror (GitHubWorkItemDeriver keeps the call "
        "inside its day loop) and this case together, under differential proof."
    )


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    # Assert the mirrored defect is still present in the producer before
    # comparing anything against it.
    _assert_producer_still_calls_inside_the_day_loop()

    days = case.get("Days")
    if not days:
        raise AssertionError(
            "the multi-day pair requires a non-empty 'Days' list; a single-day "
            "case here would silently degrade into the existing pair and prove "
            "nothing about write amplification"
        )

    # The context is built once, outside the loop, exactly as the producer does.
    # `Day` is required by DerivedCase but unused by this compute; it is set to
    # the first day so the case decodes, and the assertion below proves the
    # output genuinely does not depend on it.
    decoded = DerivedCase({**case, "Day": days[0]})

    emitted: list[Any] = []
    per_day: list[int] = []
    for _ in days:
        produced = list(decoded.team_attributions())
        per_day.append(len(produced))
        emitted.extend(produced)

    if not per_day or per_day[0] == 0:
        raise AssertionError(
            "the fixture produced no attribution rows; the multiplicity "
            "comparison would be vacuous"
        )
    if len(set(per_day)) != 1:
        raise AssertionError(
            f"per-day attribution counts differ ({per_day}); this compute is "
            "supposed to ignore the day entirely"
        )
    return columns(emitted, _fields())


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/team-attributions-multiday",
        build_row=_build_row,
        reflected_fields=_fields,
        # No org_id exclusion, for the same reason as the single-day pair:
        # compute_work_item_team_attributions copies item.org_id itself, so the
        # value is genuinely comparable on both sides.
    )
)
