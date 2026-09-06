"""Instruments for work-item team-attribution health (CHAOS-4112).

CHAOS-5321/CHAOS-3092 (R6): the downgrade counter this module used to also
carry (ATTRIBUTION_DOWNGRADES_TOTAL, build_downgrade_counter) is deleted --
its only caller, job_work_items.py's _report_attribution_downgrades, was
deleted alongside compute_work_item_team_attributions (native Go executor +
providersync ingest derivation are the only writers of
work_item_team_attributions now). The stored-edge-load-failure counter below
is unrelated (it instruments the still-live linked-issue inheritance union,
not team-attribution recompute) and stays.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "STORED_EDGE_LOAD_FAILURES_TOTAL",
    "build_stored_edge_failure_counter",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)


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
