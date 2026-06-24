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
* status == RUNNING  AND  (lease_expires_at IS NULL OR lease_expires_at > now)

RUNNING is split by lease state. A NULL lease is unknown/pre-migration and
therefore LIVE. Only an explicit expired lease proves the worker is dead; those
units no longer consume capacity and are transitioned to FAILED by the
reconciler. Stale updated_at alone is never proof of death.

**Candidate set** — units from THIS run that ``_claim_units`` can enqueue this
pass.  Mirrors ``_claim_units`` claim + reclaim logic exactly:

* status == PLANNED  (any age — claimed via UPDATE…RETURNING)
* status == DISPATCHING  AND  updated_at <= stale_dispatch_cutoff  (stale reclaim)
* status == RETRYING  AND  available_at <= now  (due deferral)

Fresh DISPATCHING is NOT a candidate (it is a consumer).
RUNNING is NOT a candidate (expired leases are terminalized, not requeued).
RETRYING never consumes capacity. Future RETRYING is deferred until available_at.

The two sets are disjoint by construction — no subtraction is needed or
performed.

Advisory locking (F1 TOCTOU fix)
---------------------------------
Before reading active_count for each bucket, ``authorize_run`` acquires a
PostgreSQL transaction-scoped advisory lock keyed on the bucket.  Buckets are
sorted deterministically before locking to prevent deadlocks when two
dispatchers race on the same set of buckets.

The lock is held until the surrounding ``get_postgres_session_sync()`` block
commits (``dispatch_sync_run`` wraps authorize→claim in one session), so the
active_count read and the subsequent ``_claim_units`` UPDATE are atomic with
respect to other dispatchers in the same bucket.

On SQLite (tests) the dialect check no-ops — advisory locks are
PostgreSQL-only.  The lock key derivation is the same deterministic 63-bit
integer pattern used in ``src/dev_health_ops/api/admin/routers/sync.py:224-239``.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import func, text

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

        # Staleness cutoff — same env var as _stale_dispatch_seconds() in
        # sync_units.py. Defined locally to avoid a circular import
        # (sync_units imports DispatchGuard from this module).
        now = datetime.now(timezone.utc)
        stale_dispatch_cutoff = now - timedelta(seconds=_stale_dispatch_seconds_guard())

        # Build the CANDIDATE set per bucket — units from THIS run that
        # _claim_units can enqueue this pass.  Mirrors _claim_units exactly:
        #   • PLANNED (any age) — claimed via UPDATE…RETURNING
        #   • stale DISPATCHING (updated_at <= stale_dispatch_cutoff) — reclaimed
        #   • due RETRYING (available_at <= now) — claimed via UPDATE…RETURNING
        # RUNNING is NOT a candidate (F2: never reclaim RUNNING — no heartbeat).
        # Fresh DISPATCHING is a capacity CONSUMER, not a candidate.
        # Future RETRYING is deferred and does not consume capacity.
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
            elif (
                unit.status == SyncRunUnitStatus.RETRYING.value
                and unit.available_at is not None
                and _as_aware_guard(unit.available_at) <= now
            ):
                candidates_by_bucket[bucket].append(unit)
            # RUNNING (any age) → capacity consumer only; never a candidate (F2).
            # Fresh DISPATCHING → consumer only (counted in active_count below).
            # Future RETRYING → deferred, no capacity consumption.
            # SUCCESS / FAILED → terminal, ignored.

        # F1 TOCTOU fix: acquire a PostgreSQL transaction-scoped advisory lock
        # per bucket BEFORE reading active_count.  Buckets are sorted
        # deterministically to prevent deadlocks when two dispatchers race on
        # the same set of buckets.  The lock is held until the surrounding
        # session commits (dispatch_sync_run wraps authorize→claim in one
        # get_postgres_session_sync() block), making the count+claim atomic.
        # On SQLite (tests) the dialect check no-ops.
        #
        # Key derivation reuses the same deterministic 63-bit integer pattern
        # as _bucket_advisory_lock_key() below (mirrors sync.py:224-239).
        all_buckets = sorted(candidates_by_bucket.keys())
        _acquire_bucket_advisory_locks(session, all_buckets)

        capped_unit_ids: list[str] = []
        for bucket, bucket_candidates in candidates_by_bucket.items():
            org_id, provider, cost_class = bucket

            # Count the CAPACITY-CONSUMER set across ALL runs (including this
            # run) for this bucket.  Consumers are:
            #   • fresh DISPATCHING: updated_at > stale_dispatch_cutoff
            #   • live RUNNING: lease_expires_at is NULL (unknown/pre-migration)
            #     or lease_expires_at > now
            # RETRYING never consumes capacity.
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
                            (SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value)
                            & (
                                SyncRunUnit.lease_expires_at.is_(None)
                                | (SyncRunUnit.lease_expires_at > now)
                            )
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


def _bucket_advisory_lock_key(org_id: str, provider: str, cost_class: str) -> int:
    """Deterministic 63-bit advisory lock key for a (org_id, provider, cost_class) bucket.

    Uses SHA-256 of the canonical bucket string, truncated to 63 bits so it
    fits in a PostgreSQL bigint (signed 64-bit).  Mirrors the pattern in
    ``src/dev_health_ops/api/admin/routers/sync.py:224-229``.
    """
    raw = f"{org_id}:{provider}:{cost_class}".encode()
    digest = hashlib.sha256(raw).digest()
    # Take first 8 bytes as big-endian unsigned int, then mask to 63 bits.
    key_int = int.from_bytes(digest[:8], "big")
    return key_int & ((1 << 63) - 1)


def _acquire_bucket_advisory_locks(
    session: Session,
    buckets: list[tuple[str, str, str]],
) -> None:
    """Acquire PostgreSQL transaction-scoped advisory locks for each bucket.

    Buckets must be pre-sorted by the caller to prevent deadlocks.
    No-ops on non-PostgreSQL dialects (e.g. SQLite in tests).
    """
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    for org_id, provider, cost_class in buckets:
        lock_key = _bucket_advisory_lock_key(org_id, provider, cost_class)
        session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": lock_key},
        )


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
# Staleness helper — mirrors sync_units._stale_dispatch_seconds() using the same
# env var. Defined here to avoid a circular import (sync_units imports
# DispatchGuard from this module).
# ---------------------------------------------------------------------------


def _stale_dispatch_seconds_guard() -> int:
    try:
        return max(
            1,
            int(os.getenv("SYNC_UNIT_DISPATCH_STALE_SECONDS", "900")),
        )
    except ValueError:
        return 900


def _as_aware_guard(value: datetime) -> datetime:
    """Return a timezone-aware UTC datetime (mirrors sync_units._as_aware)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
