"""Instruments for zero-unit sync-run finalizations.

A run that plans zero units finalizes FAILED. That is the ratified trade (see
``tests/test_sync_units.py::
test_fully_caught_up_plan_finalizes_failed_not_silently_successful``): a loud,
honest failure beats a quiet success that falsely claims coverage was
refreshed. What was NOT ratified is that every zero-unit run wears the same
label. Before CHAOS-4159 ``finalize_sync_run`` overwrote whatever cause the
planner had already recorded on the run with the generic
``"No sync units planned"``, so a PagerDuty integration with no credential, a
provider whose sync target was disabled, and a genuinely empty plan were
indistinguishable in ``sync_runs.error`` and in ``sync_runs.result``.

That flattening is why a local stack could show hundreds of identical red rows
with no way to tell which of three different causes produced them, and it is
what this counter makes visible without a database query.

``reason`` is the classification finalize actually used, never a guess:
``no_sync_units_planned`` is the residual "the planner recorded nothing", and
any other value came verbatim from the planner. A ``reason`` series that is
overwhelmingly ``no_sync_units_planned`` is itself the signal that a
zero-unit path upstream is still discarding its own diagnosis -- the Go
scheduler's ``pagerDutyCredentialUnavailable`` branch being the known case at
the time of writing.
"""

from __future__ import annotations

from typing import Any

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "ZERO_UNIT_FINALIZATIONS_TOTAL",
    "build_zero_unit_finalization_counter",
]

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)


def build_zero_unit_finalization_counter(
    *,
    meter: Any = _meter,
    prometheus: Any = _prometheus,
) -> Any:
    """Counter of sync runs finalized with zero planned units.

    Labelled by ``provider`` and by the ``reason`` finalize classified the run
    under. Provider is the axis that separates a single misconfigured
    integration from a whole route going quiet; reason is the axis that says
    whether the cause is known at all.
    """
    return build_counter(
        "devhealth_sync_run_zero_unit_finalizations_total",
        "Sync runs finalized with zero planned units, by provider and by the "
        "cause finalize classified them under",
        ["provider", "reason"],
        meter=meter,
        prometheus=prometheus,
    )


ZERO_UNIT_FINALIZATIONS_TOTAL = build_zero_unit_finalization_counter()
