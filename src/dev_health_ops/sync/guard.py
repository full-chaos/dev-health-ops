"""Dispatch-layer guard contract (CHAOS-2285 folded -> CHAOS-2512).

FROZEN CONTRACT — tier/plan + concurrency enforcement at the dispatch layer.

Today tier limits are enforced only at the API boundary; the scheduler and
worker dispatch paths bypass them. :meth:`DispatchGuard.authorize_run` MUST be
invoked at the TOP of ``dispatch_sync_run`` (after planning, before any unit is
queued) so that API, scheduler, and backfill all pass through one guard.

Decision shapes
---------------
* **Total-cap hard-deny** — ``GuardDecision(allowed=False,
  concurrency_capped=False, capped_unit_ids=(...))`` — the whole run exceeds
  the org's absolute unit ceiling.  The caller MUST mark the run FAILED.
* **Concurrency partial-cap** — ``GuardDecision(allowed=True,
  concurrency_capped=True, capped_unit_ids=(...))`` — some units cannot be
  dispatched right now because another run is consuming the per-bucket
  concurrency slots.  The caller MUST leave capped units PLANNED and schedule
  a delayed redispatch; it MUST NOT mark the run FAILED.
* **Full allow** — ``GuardDecision(allowed=True, concurrency_capped=False)``.
"""

from __future__ import annotations

import os
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import func

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class GuardDecision:
    """Outcome of an authorize check.

    Two distinct cap shapes (see module docstring):

    * **Total-cap hard-deny**: ``allowed=False, concurrency_capped=False``.
      The run exceeds the org's absolute unit ceiling; the caller must mark
      the run FAILED and stop.
    * **Concurrency partial-cap**: ``allowed=True, concurrency_capped=True``.
      Some units cannot be dispatched right now; ``capped_unit_ids`` lists
      them.  The caller must leave those units PLANNED and schedule a delayed
      redispatch — it must NOT mark the run FAILED.
    * **Full allow**: ``allowed=True, concurrency_capped=False``.
    """

    allowed: bool
    reason: str | None = None
    capped_unit_ids: tuple[str, ...] = field(default_factory=tuple)
    concurrency_capped: bool = False


class DispatchGuard:
    """Gates total unit count, cost-class totals, and active concurrency."""

    @staticmethod
    def authorize_run(session: Session, sync_run_id: str) -> GuardDecision:
        """Authorize (or cap) a planned run before its units are queued.

        Reads the persisted plan and meters against org/provider/cost-class
        budgets. Implemented in CHAOS-2512.
        """

        from dev_health_ops.models import SyncRun, SyncRunUnit, SyncRunUnitStatus

        run_uuid = uuid.UUID(str(sync_run_id))
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            return GuardDecision(False, f"sync_run not found: {sync_run_id}")

        units = (
            session.query(SyncRunUnit)
            .filter(SyncRunUnit.sync_run_id == run_uuid)
            .order_by(SyncRunUnit.id)
            .all()
        )
        total_cap = _resolve_total_unit_cap(session, str(run.org_id))
        if len(units) > total_cap:
            capped = tuple(str(unit.id) for unit in units[total_cap:])
            return GuardDecision(
                False,
                f"sync run unit cap exceeded: {len(units)}/{total_cap}",
                capped,
            )

        concurrency_cap = _env_int("SYNC_UNIT_CONCURRENCY_PER_BUCKET", 8)
        planned_by_bucket: dict[tuple[str, str, str], list[SyncRunUnit]] = defaultdict(
            list
        )
        eligible_statuses = {
            SyncRunUnitStatus.PLANNED.value,
            SyncRunUnitStatus.DISPATCHING.value,
        }
        for unit in units:
            if unit.status in eligible_statuses:
                planned_by_bucket[
                    (str(unit.org_id), unit.provider, unit.cost_class)
                ].append(unit)

        capped_unit_ids: list[str] = []
        active_statuses = {
            SyncRunUnitStatus.DISPATCHING.value,
            SyncRunUnitStatus.RUNNING.value,
            SyncRunUnitStatus.RETRYING.value,
        }
        for bucket, bucket_units in planned_by_bucket.items():
            org_id, provider, cost_class = bucket
            active_count = (
                session.query(func.count(SyncRunUnit.id))
                .filter(
                    SyncRunUnit.sync_run_id != run_uuid,
                    SyncRunUnit.org_id == org_id,
                    SyncRunUnit.provider == provider,
                    SyncRunUnit.cost_class == cost_class,
                    SyncRunUnit.status.in_(active_statuses),
                )
                .scalar()
                or 0
            )
            allowed_slots = max(0, concurrency_cap - int(active_count))
            if len(bucket_units) > allowed_slots:
                capped_unit_ids.extend(
                    str(unit.id) for unit in bucket_units[allowed_slots:]
                )

        if capped_unit_ids:
            return GuardDecision(
                True,
                f"sync unit concurrency cap exceeded: {len(capped_unit_ids)} capped",
                tuple(capped_unit_ids),
                concurrency_capped=True,
            )

        return GuardDecision(True)


def _resolve_total_unit_cap(session: Session, org_id: str) -> int:
    default_cap = _env_int("SYNC_RUN_MAX_UNITS", 1000)
    try:
        from dev_health_ops.api.services.licensing import TierLimitService

        tier_cap = TierLimitService(session).get_limit(
            uuid.UUID(org_id), "max_sync_units"
        )
    except Exception:
        return default_cap
    if tier_cap is None:
        return default_cap
    return int(tier_cap)


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(1, value)
