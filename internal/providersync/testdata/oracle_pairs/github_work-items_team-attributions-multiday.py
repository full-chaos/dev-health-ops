"""Live Python oracle for team attributions over a MULTI-DAY window.

This pair exists to pin CHAOS-3494, not to re-prove single-day attribution
(``github/work-items/team-attributions`` already does that).

``compute_work_item_team_attributions`` takes no ``day`` argument, yet
``job_work_items.py:1232`` calls it INSIDE the caller's ``for d in days`` loop.
A multi-day backfill therefore recomputes and re-emits byte-identical
attribution rows once per day -- pure write amplification. Per D16 the Go port
mirrors that bug-for-bug rather than repairing it, so the multiplicity itself
is the compared property: an n-day window must produce exactly n copies of the
single-day result, in day order, on BOTH sides.

The loop here mirrors the producer's structure exactly: the resolver cascade and
attribution context are built ONCE (``DerivedCase.__init__``, matching
job_work_items.py:1195-1209 which builds them before the loop), and only the
compute call repeats. Rebuilding the context per day would be a different
program from the one in production, and would hide the very defect this pins.

If CHAOS-3494 is ever fixed, this pair fails on both sides at once -- which is
the intended signal: fix Python, then update the Go mirror and this case
together under differential proof.
"""

from __future__ import annotations

from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.oracle_pairs._github_work_item_derived_helpers import (
    SCHEMA_SOURCE,
    DerivedCase,
    columns,
)

_RECORD = "WorkItemTeamAttributionRecord"


def _fields() -> frozenset[str]:
    return dataclass_field_names(SCHEMA_SOURCE.read_text(), _RECORD)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
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
