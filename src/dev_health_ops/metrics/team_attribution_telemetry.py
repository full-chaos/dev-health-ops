"""Instruments for work-item team-attribution health (CHAOS-4112).

Attribution rows are recomputed and re-stamped on every run, so a regression
does not announce itself: the new row simply supersedes the old one and the
work item quietly loses its team. The one transition that is always a bug is a
DOWNGRADE -- an item whose primary attribution came from a real team source
resolving to `unassigned` on a later run. That is the decay signature
CHAOS-4112 describes, and per the standing telemetry order it must never be
silent.

Only downgrades are counted. An item going from `unassigned` to a team is a
recovery, and an item moving between two teamed sources is a precedence
change; neither is a data loss.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "ATTRIBUTION_DOWNGRADES_TOTAL",
    "STORED_EDGE_LOAD_FAILURES_TOTAL",
    "build_downgrade_counter",
    "build_stored_edge_failure_counter",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)


def build_downgrade_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of teamed -> unassigned attribution transitions.

    ``previous_source`` is the source the item's primary attribution had
    BEFORE this run, so the series says which kind of fact was lost --
    `linked_issue` decay (the CHAOS-4112 mechanism, an edge aging out of the
    sync window) looks different from a `native_team` disappearing, which
    would point at provider data rather than at this code.
    """
    return build_counter(
        "devhealth_work_item_team_attribution_downgrades_total",
        "Work items whose primary team attribution fell from a teamed source "
        "to unassigned across a recompute",
        ["provider", "previous_source"],
        meter=meter,
        prometheus=prometheus,
    )


ATTRIBUTION_DOWNGRADES_TOTAL = build_downgrade_counter()


def build_stored_edge_failure_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of runs that could not read the stored inheritable edges.

    When this read fails the recompute falls back to the sync window alone --
    the pre-CHAOS-4112 behaviour -- so for that one run an item whose donor
    edge is older than the window can be rebuilt as `unassigned`. The next
    successful run restores it, because the stored edge is found again, so the
    regression is transient and self-healing rather than permanent. It is
    still a window in which attributions degrade, so it must be visible:
    a non-zero rate here explains a spike in
    `devhealth_work_item_team_attribution_downgrades_total` that is an
    infrastructure symptom rather than an attribution bug.
    """
    return build_counter(
        "devhealth_work_item_stored_edge_load_failures_total",
        "Metrics runs whose stored inheritable-edge read failed, leaving team "
        "inheritance limited to the sync window for that run",
        ["org_id"],
        meter=meter,
        prometheus=prometheus,
    )


STORED_EDGE_LOAD_FAILURES_TOTAL = build_stored_edge_failure_counter()
