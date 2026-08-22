"""The single runtime-selection rule for a provider sync unit (CHAOS-3941).

Before this module there were two independent sources of truth about which
provider/dataset pairs the system would act on:

  * the PLANNER decided which units to CREATE, from ``IntegrationDataset``
    rows (per-org, customer-facing, in Postgres), and
  * the DISPATCHER decided where to SEND them, from a per-process bank of
    ``WORKER_*_ENABLED`` environment switches, defaulting to False for
    anything unset.

CHAOS-4054 deleted that second plane entirely -- capability is always on in the
binary and routability is a property of the capability matrix alone -- so the
two can no longer disagree by construction. This module survives only to select
between River and the CUT-19 Celery rollback profile; it is retired with Celery
under CHAOS-4026.

Nothing asserted the two agreed. When they disagreed the planner created a
unit that no runtime would execute: not River (its switch was off) and not
Celery (scaled to zero). Those units were published into a broker with no
consumer, wedged in ``dispatching``, consumed the whole DispatchGuard
concurrency budget, and never aged out because each dispatch pass re-stamped
``updated_at`` under the stale reaper's cutoff.

Both callers now ask THIS function, with the same inputs, and act on the same
answer:

  * the planner does not CREATE a unit that resolves to ``UNROUTABLE``;
  * the dispatcher TERMINALIZES as ``feature_disabled`` any unit that still
    resolves to ``UNROUTABLE`` (planned before a switch flip, or planned while
    the plan-time gate was unavailable) instead of publishing it into the void.

The Celery fallthrough is NOT removed. CUT-19 (CHAOS-3091, rollback rehearsal)
requires starting "the complete Celery fallback profile" and executing at least
one representative job through it, and the canary state deliberately runs a
mixed transport. Both states report ``CeleryConsumerPresence.PRESENT``, under
which this resolver still selects Celery exactly as before. What is removed is
the publish that happens when Celery is *provably* not there.
"""

from __future__ import annotations

import enum
from collections.abc import Iterable
from typing import Protocol

from dev_health_ops.sync.canonical_incident_gate import (
    FEATURE_DISABLED_ERROR_CATEGORY,
)
from dev_health_ops.sync.dispatch_policy import route
from dev_health_ops.workers import celery_consumers as _celery_consumers
from dev_health_ops.workers.celery_consumers import CeleryConsumerPresence
from dev_health_ops.workers.job_routes import RIVER_CANARY_ROUTE, RIVER_ROUTE
from dev_health_ops.workers.provider_unit_route import routes_to_river
from dev_health_ops.workers.queues import _cost_class_queues_enabled

#: The durable ``sync.provider_unit`` routes under which River owns provider
#: units at all. Any other route (``celery``) means Celery owns every provider
#: unit -- the CUT-19 full-rollback state.
PROVIDER_UNIT_OUTBOX_ROUTES = frozenset({RIVER_CANARY_ROUTE, RIVER_ROUTE})

#: Error/category stamped on a unit no runtime can execute. Reuses the
#: codebase's existing terminal-denial idiom (``sync/feature_denial.py``,
#: ``sync/dispatch_outbox.py``) so downstream readers need no new vocabulary.
UNROUTABLE_ERROR_CATEGORY = FEATURE_DISABLED_ERROR_CATEGORY


class UnitTransport(enum.Enum):
    """Which runtime may execute a provider sync unit."""

    RIVER = "river"
    CELERY = "celery"
    #: Confirmed: no runtime will run this. Terminalize it.
    UNROUTABLE = "unroutable"
    #: Cannot tell right now. Do nothing destructive and do not publish; put
    #: the unit back and look again next pass.
    DEFER = "defer"


def resolve_unit_transport(
    provider: str,
    dataset: str,
    *,
    river_owns_units: bool,
    celery_presence: CeleryConsumerPresence,
) -> UnitTransport:
    """Resolve the one runtime that may execute ``(provider, dataset)``.

    ``river_owns_units`` is ``False`` when the durable ``sync.provider_unit``
    route is not a River outbox route -- Celery owns everything, unchanged.

    ``UNROUTABLE`` is returned only when River declines the pair AND the
    broker confirmed that nothing consumes the queue this unit would land in.

    ``UNKNOWN`` does NOT fall back to publishing (review finding). The earlier
    reasoning -- "an unreachable broker makes apply_async raise anyway" -- has
    a hole: the broker can be perfectly reachable while only the pidbox control
    plane fails. Then the probe cannot see the consumers, ``apply_async``
    succeeds, and the message lands in an empty queue: the original bug,
    restored by the very guard meant to close it. So an unknown consumer state
    defers instead, which neither publishes into a possible void nor destroys
    work on a guess.
    """

    if river_owns_units and routes_to_river(provider, dataset):
        return UnitTransport.RIVER
    if celery_presence is CeleryConsumerPresence.ABSENT:
        return UnitTransport.UNROUTABLE
    if celery_presence is CeleryConsumerPresence.UNKNOWN:
        return UnitTransport.DEFER
    return UnitTransport.CELERY


class RoutableUnit(Protocol):
    """The four fields both a ``PlannedUnit`` and a ``SyncRunUnit`` expose.

    Read-only properties, not bare attributes: ``PlannedUnit`` is a FROZEN
    dataclass, and a Protocol declaring mutable attributes would refuse it.
    """

    @property
    def org_id(self) -> str: ...

    @property
    def provider(self) -> str: ...

    @property
    def dataset_key(self) -> str: ...

    @property
    def cost_class(self) -> str: ...


def celery_fallback_queues(
    units: Iterable[RoutableUnit],
    *,
    river_owns_units: bool,
) -> frozenset[str]:
    """The Celery queues these units would actually be published to.

    Queue knowledge lives HERE, not in the planner: ``dispatch_policy`` is a
    frozen contract stating "the planner does NOT know about queues", and the
    planner still does not -- it hands over units and gets back a transport.
    """

    queues: set[str] = set()
    cost_class_queues_enabled = _cost_class_queues_enabled()
    for unit in units:
        if river_owns_units and routes_to_river(unit.provider, unit.dataset_key):
            continue
        queues.add(
            route(
                org_id=str(unit.org_id),
                provider=str(unit.provider),
                cost_class=str(unit.cost_class),
                cost_class_queues_enabled=cost_class_queues_enabled,
            ).queue
        )
    return frozenset(queues)


def resolve_celery_presence(
    units: Iterable[RoutableUnit],
    *,
    river_owns_units: bool,
) -> CeleryConsumerPresence:
    """Probe Celery consumers, but only when some unit would need them.

    In the healthy cut-over steady state every unit routes to River, so the
    broadcast never runs. The probe cost is paid exactly when a unit is about
    to depend on the Celery fallthrough -- the situation this guard exists for.

    The probe is asked about the SPECIFIC queues those units would land in. A
    worker consuming an unrelated queue is not a consumer of yours: this
    deployment keeps other Celery workers running long after the provider-unit
    queue is empty, so a "is any worker up" question would answer PRESENT and
    put the units straight back into the void.
    """

    queues = celery_fallback_queues(units, river_owns_units=river_owns_units)
    if not queues:
        # Nothing needs the fallthrough: never pay for a broadcast. PRESENT,
        # not UNKNOWN -- an unasked question must not defer River-bound work.
        return CeleryConsumerPresence.PRESENT
    return _celery_consumers.probe_celery_consumers(queues)


__all__ = [
    "PROVIDER_UNIT_OUTBOX_ROUTES",
    "UNROUTABLE_ERROR_CATEGORY",
    "CeleryConsumerPresence",
    "RoutableUnit",
    "UnitTransport",
    "celery_fallback_queues",
    "resolve_celery_presence",
    "resolve_unit_transport",
]
