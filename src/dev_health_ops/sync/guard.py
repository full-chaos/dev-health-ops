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
from datetime import datetime, timedelta, timezone
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

        # Units from THIS run that are candidates for dispatch this pass:
        # only PLANNED and DISPATCHING (fresh DISPATCHING will be reclaimed by
        # _claim_units; RUNNING/RETRYING from this run are already in-flight
        # and must count against the cap, not be re-dispatched).
        dispatch_candidate_statuses = {
            SyncRunUnitStatus.PLANNED.value,
            SyncRunUnitStatus.DISPATCHING.value,
        }
        planned_by_bucket: dict[tuple[str, str, str], list[SyncRunUnit]] = defaultdict(
            list
        )
        for unit in units:
            if unit.status in dispatch_candidate_statuses:
                planned_by_bucket[
                    (str(unit.org_id), unit.provider, unit.cost_class)
                ].append(unit)

        # Freshness cutoffs — same semantics as _stale_dispatch_seconds() /
        # _stale_running_seconds() in sync_units.py.  We do NOT import
        # sync_units here (circular: sync_units imports DispatchGuard).
        now = datetime.now(timezone.utc)
        stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds_guard())
        stale_running_cutoff = now - timedelta(seconds=_stale_running_seconds_guard())

        # Active-slot statuses: units that are genuinely occupying a concurrency
        # slot right now (across ALL runs, including the current run).
        # DISPATCHING/RUNNING/RETRYING from the current run are already in-flight
        # and must reduce available capacity — they must NOT be re-dispatched.
        active_statuses = {
            SyncRunUnitStatus.DISPATCHING.value,
            SyncRunUnitStatus.RUNNING.value,
            SyncRunUnitStatus.RETRYING.value,
        }

        capped_unit_ids: list[str] = []
        for bucket, bucket_units in planned_by_bucket.items():
            org_id, provider, cost_class = bucket

            # Count fresh active units across ALL runs (including this one).
            # A unit is "fresh" if its updated_at is within the staleness window
            # for its status — stale units will be reclaimed by _claim_units and
            # should not permanently block capacity.
            #
            # F1: include same-run RUNNING/RETRYING/DISPATCHING so they reduce
            #     available slots (previously excluded via != run_uuid).
            # F2: apply freshness thresholds so dead-worker leftovers don't cap
            #     forever (previously no updated_at filter).
            active_count = (
                session.query(func.count(SyncRunUnit.id))
                .filter(
                    SyncRunUnit.org_id == org_id,
                    SyncRunUnit.provider == provider,
                    SyncRunUnit.cost_class == cost_class,
                    SyncRunUnit.status.in_(active_statuses),
                    # Exclude units that are stale (dead worker) — they will be
                    # reclaimed and must not permanently block capacity.
                    # DISPATCHING units stale after stale_dispatch_cutoff,
                    # RUNNING/RETRYING units stale after stale_running_cutoff.
                    # We use the more conservative (longer) running cutoff for
                    # RETRYING since it shares the same semantics as RUNNING.
                    (
                        (SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value)
                        & (SyncRunUnit.updated_at > stale_dispatch_cutoff)
                        | (
                            SyncRunUnit.status.in_(
                                {
                                    SyncRunUnitStatus.RUNNING.value,
                                    SyncRunUnitStatus.RETRYING.value,
                                }
                            )
                        )
                        & (SyncRunUnit.updated_at > stale_running_cutoff)
                    ),
                )
                .scalar()
                or 0
            )

            # Subtract same-run dispatch candidates that are DISPATCHING
            # (they are already counted in active_count above but will be
            # reclaimed by _claim_units, so they don't consume a net new slot).
            # RUNNING/RETRYING from this run are genuinely in-flight and DO
            # consume slots — do not subtract them.
            same_run_fresh_dispatching = sum(
                1
                for u in bucket_units
                if u.status == SyncRunUnitStatus.DISPATCHING.value
                and _as_aware_guard(u.updated_at) > stale_dispatch_cutoff
            )
            # Net active slots consumed by OTHER units (not the candidates we
            # are about to dispatch).
            net_active = int(active_count) - same_run_fresh_dispatching
            allowed_slots = max(0, concurrency_cap - net_active)

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


# ---------------------------------------------------------------------------
# Staleness helpers — mirrors sync_units._stale_dispatch_seconds() /
# _stale_running_seconds() using the same env vars.  Defined here to avoid
# a circular import (sync_units imports DispatchGuard from this module).
# ---------------------------------------------------------------------------


def _stale_dispatch_seconds_guard() -> int:
    try:
        return max(
            1,
            int(os.getenv("SYNC_UNIT_DISPATCH_STALE_SECONDS", "900")),
        )
    except ValueError:
        return 900


def _stale_running_seconds_guard() -> int:
    try:
        return max(
            1,
            int(os.getenv("SYNC_UNIT_RUNNING_STALE_SECONDS", "3600")),
        )
    except ValueError:
        return 3600


def _as_aware_guard(value: datetime) -> datetime:
    """Return a timezone-aware UTC datetime (mirrors sync_units._as_aware)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
