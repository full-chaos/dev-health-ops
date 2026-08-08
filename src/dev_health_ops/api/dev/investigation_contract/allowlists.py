"""Trial source allowlist and current-versus-historical slice boundaries.

CHAOS-3615 deliverables 9 and 10.

The **source allowlist** names which of the platform's ``SourceClass``
members an investigation arm may draw on during the corrected trial, and why
each one is in. It is a strict subset of ``SourceClass``: the enum carries
members that exist for the Ask Dev *answer* path and have no role in
subject/cohort/lineage/driver discovery, and quietly permitting them would
let an arm claim coverage from a source the trial never intended to score.
``TEMPORAL_CONTEXT`` is excluded for a different reason — it is CHAOS-3567's
deliberately inert flag-off stub, and pulling it into this allowlist would
make it non-inert by the back door.

The **slice boundaries** say what "current" and "historical" each require of
edge validity, and record the CHAOS-3569 consequence explicitly: native
historical edge validity is not implemented, so a historical or
current-vs-historical slice that needs as-of traversal over gapped rows is
``NOT COMPARABLE``. Per the corrective plan that is a *reporting* state, not
a blocker — the packet stays valid, the trial row is scored NOT COMPARABLE
rather than failed, and the arm must disclose the limitation.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

from dev_health_ops.api.dev.contracts import Label, LongText, ShortText
from dev_health_ops.api.dev.contracts_v2.base import ContractModelV2, SourceClass

from .vocabulary import (
    ALL_ANALYTICAL_SLICES,
    AnalyticalSlice,
    HistoricalComparability,
)

__all__ = [
    "ALL_ANALYTICAL_SLICES",
    "SLICE_BOUNDARIES",
    "TRIAL_SOURCE_ALLOWLIST",
    "TRIAL_SOURCE_RATIONALE",
    "SliceBoundary",
    "validate_slice_boundaries",
    "validate_trial_source_allowlist",
]


#: The source classes an investigation arm may draw on during the trial.
#:
#: Ordered as declared on ``SourceClass`` so the exported schemas and the
#: registry documentation stay byte-stable.
TRIAL_SOURCE_ALLOWLIST: tuple[SourceClass, ...] = (
    SourceClass.STATUS_CHANGE,
    SourceClass.WORK_ITEM,
    SourceClass.WORK_GRAPH,
    SourceClass.PULL_REQUEST,
    SourceClass.CODE_CHANGE,
    SourceClass.REVIEW,
    SourceClass.CI_RUN,
    SourceClass.TEST_REPORT,
    SourceClass.DEPLOYMENT,
    SourceClass.INCIDENT,
    SourceClass.OPERATIONAL_CONTROL,
    SourceClass.SOURCE_HEALTH,
    SourceClass.COGNITIVE_LOAD,
    SourceClass.INVESTMENT_ALLOCATION,
    SourceClass.HEALTH_PROFILE,
    SourceClass.DEFICIENCY_INVENTORY,
)

#: Why each allowed source is in the trial. Totality against
#: :data:`TRIAL_SOURCE_ALLOWLIST` is enforced by
#: :func:`validate_trial_source_allowlist`, so a source added to the
#: allowlist without a stated reason is an import-time failure.
TRIAL_SOURCE_RATIONALE: Mapping[SourceClass, str] = {
    SourceClass.STATUS_CHANGE: (
        "declared status and its transitions -- the 'declared' half of the "
        "declared-versus-actual family"
    ),
    SourceClass.WORK_ITEM: (
        "canonical units of work; the substrate for delivery pressure, scope "
        "change and blocked-by lineage"
    ),
    SourceClass.WORK_GRAPH: (
        "canonical entity identity and membership -- how a candidate subject "
        "becomes a canonical ID at all"
    ),
    SourceClass.PULL_REQUEST: (
        "implementing changes; the 'actual delivery evidence' half of the "
        "declared-versus-actual family"
    ),
    SourceClass.CODE_CHANGE: "change volume and locality behind delivery pressure",
    SourceClass.REVIEW: "review pressure, and the cycled-in-review colloquial family",
    SourceClass.CI_RUN: "readiness evidence for declared-complete claims",
    SourceClass.TEST_REPORT: "readiness evidence for declared-complete claims",
    SourceClass.DEPLOYMENT: "release evidence for declared-complete claims",
    SourceClass.INCIDENT: "operational pressure and its driver lineage",
    SourceClass.OPERATIONAL_CONTROL: (
        "operational readiness controls behind operational-deficiency drivers"
    ),
    SourceClass.SOURCE_HEALTH: (
        "source availability and freshness -- required to distinguish 'no "
        "signal' from 'no data'"
    ),
    SourceClass.COGNITIVE_LOAD: (
        "team-level interruption/context-spread/review-request rollups behind "
        "the struggling-teams family. Team-level only: this source class also "
        "backs per-developer rollups, which the packet cannot express because "
        "no person subject kind exists"
    ),
    SourceClass.INVESTMENT_ALLOCATION: (
        "investment mix pressure, and the nearest available proxy for a "
        "capacity denominator"
    ),
    SourceClass.HEALTH_PROFILE: (
        "code-owned health-rule findings; a derived judgment over other "
        "sources, never a primary source of its own"
    ),
    SourceClass.DEFICIENCY_INVENTORY: (
        "operational-deficiency inventory behind readiness and operational "
        "pressure judgments"
    ),
}


class SliceBoundary(ContractModelV2):
    """What one analytical slice requires, and what it cannot yet deliver."""

    schema_version: Literal["ask_dev_slice_boundary.v1"]
    slice_id: AnalyticalSlice
    title: Label
    definition: LongText
    requires_as_of: bool
    requires_edge_validity: bool
    permitted_comparability: tuple[HistoricalComparability, ...]
    known_gap: ShortText | None


SLICE_BOUNDARIES: Mapping[AnalyticalSlice, SliceBoundary] = {
    AnalyticalSlice.CURRENT: SliceBoundary(
        schema_version="ask_dev_slice_boundary.v1",
        slice_id=AnalyticalSlice.CURRENT,
        title="Current",
        definition=(
            "Entities, relationships and evidence that are valid now. The "
            "investigation reads the live projection: no as-of timestamp is "
            "supplied and no edge-validity interval is consulted. This is the "
            "slice every 'what is happening / what is going sideways / who "
            "needs attention' question runs in, and it is deliberately the "
            "slice that CHAOS-3569 does not block."
        ),
        requires_as_of=False,
        requires_edge_validity=False,
        permitted_comparability=(HistoricalComparability.NOT_APPLICABLE,),
        known_gap=None,
    ),
    AnalyticalSlice.HISTORICAL: SliceBoundary(
        schema_version="ask_dev_slice_boundary.v1",
        slice_id=AnalyticalSlice.HISTORICAL,
        title="Historical",
        definition=(
            "The state as of a supplied past instant. Requires an as-of "
            "timestamp and requires every traversed edge to carry a validity "
            "interval covering that instant, so that relationships which did "
            "not exist then are not reported as if they did."
        ),
        requires_as_of=True,
        requires_edge_validity=True,
        permitted_comparability=(
            HistoricalComparability.COMPARABLE,
            HistoricalComparability.NOT_COMPARABLE_MISSING_EDGE_VALIDITY,
            HistoricalComparability.NOT_COMPARABLE_MISSING_BASELINE,
        ),
        known_gap=(
            "CHAOS-3569 (native historical edge validity) is open. Rows whose "
            "edges lack a validity interval are reported NOT COMPARABLE and "
            "excluded from historical scoring -- they are not trial failures "
            "and they do not block the current slice."
        ),
    ),
    AnalyticalSlice.CURRENT_VS_HISTORICAL: SliceBoundary(
        schema_version="ask_dev_slice_boundary.v1",
        slice_id=AnalyticalSlice.CURRENT_VS_HISTORICAL,
        title="Current versus historical",
        definition=(
            "A change question: the current state compared against the state "
            "as of a supplied past instant. Inherits every requirement of the "
            "historical slice, plus a baseline that must itself be "
            "reconstructable -- a comparison against a baseline that cannot "
            "be rebuilt is NOT COMPARABLE rather than a zero delta."
        ),
        requires_as_of=True,
        requires_edge_validity=True,
        permitted_comparability=(
            HistoricalComparability.COMPARABLE,
            HistoricalComparability.NOT_COMPARABLE_MISSING_EDGE_VALIDITY,
            HistoricalComparability.NOT_COMPARABLE_MISSING_BASELINE,
        ),
        known_gap=(
            "CHAOS-3569 as above, plus: a missing baseline must be reported "
            "NOT_COMPARABLE_MISSING_BASELINE and never silently rendered as "
            "'no change'."
        ),
    ),
}


def validate_trial_source_allowlist() -> None:
    """Raise unless the source allowlist is a sane, fully-justified subset."""

    allowlist = set(TRIAL_SOURCE_ALLOWLIST)
    if len(TRIAL_SOURCE_ALLOWLIST) != len(allowlist):
        raise RuntimeError("trial source allowlist repeats a source class")
    platform_sources = set(SourceClass.__members__.values())
    outside = sorted(str(item) for item in allowlist - platform_sources)
    if outside:
        raise RuntimeError(f"trial source allowlist names unknown sources: {outside}")
    unjustified = sorted(str(item) for item in allowlist - set(TRIAL_SOURCE_RATIONALE))
    if unjustified:
        raise RuntimeError(
            f"trial source allowlist entries have no stated rationale: {unjustified}"
        )
    orphaned = sorted(str(item) for item in set(TRIAL_SOURCE_RATIONALE) - allowlist)
    if orphaned:
        raise RuntimeError(
            f"trial source rationale names non-allowlisted sources: {orphaned}"
        )
    if SourceClass.TEMPORAL_CONTEXT in allowlist:
        # CHAOS-3567 shipped this member deliberately inert. Allowlisting it
        # here would make it reachable without any of the plan/step wiring
        # that inertness guard proves absent.
        raise RuntimeError(
            "SourceClass.TEMPORAL_CONTEXT is CHAOS-3567's inert stub and must "
            "not be allowlisted for the trial"
        )


def validate_slice_boundaries() -> None:
    """Raise unless every analytical slice has a coherent declared boundary."""

    if set(SLICE_BOUNDARIES) != set(ALL_ANALYTICAL_SLICES):
        missing = sorted(
            str(item) for item in set(ALL_ANALYTICAL_SLICES) - set(SLICE_BOUNDARIES)
        )
        extra = sorted(
            str(item) for item in set(SLICE_BOUNDARIES) - set(ALL_ANALYTICAL_SLICES)
        )
        raise RuntimeError(
            f"slice boundaries are not total; missing={missing}, extra={extra}"
        )
    for slice_id, boundary in SLICE_BOUNDARIES.items():
        if boundary.slice_id is not slice_id:
            raise RuntimeError(
                f"slice boundary key {slice_id} is filed under {boundary.slice_id}"
            )
        if not boundary.permitted_comparability:
            raise RuntimeError(f"slice {slice_id} permits no comparability state")
        if boundary.requires_edge_validity and not boundary.known_gap:
            # Every slice that needs edge validity today runs into CHAOS-3569.
            # Dropping the gap note would let a future edit quietly imply the
            # historical slice is fully supported.
            raise RuntimeError(
                f"slice {slice_id} requires edge validity but declares no "
                "known gap; CHAOS-3569 is open and must stay disclosed"
            )
        applicable = HistoricalComparability.NOT_APPLICABLE
        if boundary.requires_as_of and applicable in boundary.permitted_comparability:
            raise RuntimeError(
                f"slice {slice_id} requires an as-of instant, so "
                "'not_applicable' comparability is not a legal state for it"
            )
        if not boundary.requires_as_of and boundary.permitted_comparability != (
            applicable,
        ):
            raise RuntimeError(
                f"slice {slice_id} needs no as-of instant, so 'not_applicable' "
                "is its only legal comparability state"
            )


validate_trial_source_allowlist()
validate_slice_boundaries()
