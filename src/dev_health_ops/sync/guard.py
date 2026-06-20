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

Concurrency model
-----------------
Two disjoint sets per (org_id, provider, cost_class) bucket:

**Capacity-consumer set** — units that are genuinely occupying a concurrency
slot right now, across ALL runs (including the current run).  These reduce
``allowed_slots``:

* status == DISPATCHING  AND  updated_at > stale_dispatch_cutoff  (fresh)
* status in {RUNNING, RETRYING}  AND  updated_at > stale_running_cutoff  (fresh)

Stale units are excluded because ``_claim_units`` will reclaim them on the
next pass; they must not permanently block capacity.

**Candidate set** — units from THIS run that ``_claim_units`` can enqueue this
pass.  Mirrors ``_claim_units`` claim + reclaim logic exactly:

* status == PLANNED  (any age — claimed via UPDATE…RETURNING)
* status == DISPATCHING  AND  updated_at <= stale_dispatch_cutoff  (stale reclaim)
* status == RUNNING  AND  updated_at <= stale_running_cutoff  (stale reclaim)

Fresh DISPATCHING is NOT a candidate (it is a consumer).
Stale RETRYING is NOT a candidate (``_claim_units`` does not reclaim RETRYING).

The two sets are disjoint by construction — no subtraction is needed or
performed.
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

        # Load all units for this run, ordered by id (stable ordering for cap suffix).
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

        # Staleness cutoffs — same env vars as _stale_dispatch_seconds() /
        # _stale_running_seconds() in sync_units.py.  Defined locally to avoid
        # a circular import (sync_units imports DispatchGuard from this module).
        now = datetime.now(timezone.utc)
        stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds_guard())
        stale_running_cutoff = now - timedelta(seconds=_stale_running_seconds_guard())

        # Build the CANDIDATE set per bucket — units from THIS run that
        # _claim_units can enqueue this pass.  Mirrors _claim_units exactly:
        #   • PLANNED (any age) — claimed via UPDATE…RETURNING
        #   • stale DISPATCHING (updated_at <= stale_dispatch_cutoff) — reclaimed
        #   • stale RUNNING (updated_at <= stale_running_cutoff) — reclaimed
        # Fresh DISPATCHING is a capacity CONSUMER, not a candidate.
        # Stale RETRYING is neither (_claim_units does not reclaim RETRYING).
        candidates_by_bucket: dict[tuple[str, str, str], list[SyncRunUnit]] = (
            defaultdict(list)
        )
        for unit in units:
            bucket = (str(unit.org_id), unit.provider, unit.cost_class)
            if unit.status == SyncRunUnitStatus.PLANNED.value:
                candidates_by_bucket[bucket].append(unit)
            elif unit.status == SyncRunUnitStatus.DISPATCHING.value:
                if _as_aware_guard(unit.updated_at) <= stale_dispatch_cutoff:
                    # Stale DISPATCHING — _claim_units will reclaim it.
                    candidates_by_bucket[bucket].append(unit)
            elif unit.status == SyncRunUnitStatus.RUNNING.value:
                if _as_aware_guard(unit.updated_at) <= stale_running_cutoff:
                    # Stale RUNNING — _claim_units will reclaim it.
                    candidates_by_bucket[bucket].append(unit)
            # Fresh DISPATCHING → consumer only (counted in active_count below).
            # RETRYING (any age) → neither candidate nor consumer for this run.
            # SUCCESS / FAILED → terminal, ignored.

        capped_unit_ids: list[str] = []
        for bucket, bucket_candidates in candidates_by_bucket.items():
            org_id, provider, cost_class = bucket

            # Count the CAPACITY-CONSUMER set across ALL runs (including this
            # run) for this bucket.  Consumers are fresh active units:
            #   • fresh DISPATCHING: updated_at > stale_dispatch_cutoff
            #   • fresh RUNNING or RETRYING: updated_at > stale_running_cutoff
            # Stale units are excluded — they will be reclaimed and must not
            # permanently block capacity (F2 stale-blocker fix).
            # Same-run fresh DISPATCHING IS included — it is a genuine consumer
            # that reduces available slots (F1 same-run visibility fix).
            # No subtraction is performed; the consumer and candidate sets are
            # disjoint by construction.
            active_count = (
                session.query(func.count(SyncRunUnit.id))
                .filter(
                    SyncRunUnit.org_id == org_id,
                    SyncRunUnit.provider == provider,
                    SyncRunUnit.cost_class == cost_class,
                    (
                        (
                            (SyncRunUnit.status == SyncRunUnitStatus.DISPATCHING.value)
                            & (SyncRunUnit.updated_at > stale_dispatch_cutoff)
                        )
                        | (
                            SyncRunUnit.status.in_(
                                {
                                    SyncRunUnitStatus.RUNNING.value,
                                    SyncRunUnitStatus.RETRYING.value,
                                }
                            )
                            & (SyncRunUnit.updated_at > stale_running_cutoff)
                        )
                    ),
                )
                .scalar()
                or 0
            )

            allowed_slots = max(0, concurrency_cap - int(active_count))
            if len(bucket_candidates) > allowed_slots:
                capped_unit_ids.extend(
                    str(unit.id) for unit in bucket_candidates[allowed_slots:]
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
