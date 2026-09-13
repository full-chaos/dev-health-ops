from __future__ import annotations

import os
import time
import uuid
from datetime import datetime
from typing import Any

from dev_health_ops.models import (
    SyncRun,
    SyncRunReferenceDiscovery,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    upsert_outbox_wakeup,
)

DISCOVERY_STATUS_PLANNED = "planned"
DISCOVERY_STATUS_RUNNING = "running"
DISCOVERY_STATUS_RETRYING = "retrying"
DISCOVERY_STATUS_SUCCESS = "success"
DISCOVERY_STATUS_FAILED = "failed"


def await_reference_discovery_terminal(
    sync_run_id: str,
    *,
    poll_interval: float = 0.5,
) -> dict[str, Any]:
    """Poll ``sync_run_reference_discoveries.status`` for one sync run until
    it reaches a terminal state, bounded by the SAME lease/lifetime
    constants ``NativeReferenceDiscoveryService`` (Go) uses for its own
    claim/execution deadlines (CHAOS-4498) -- never an invented constant.
    A caller (the operator backfill tool) gets a typed outcome for every
    exit and must NEVER fall back to calling the Python populator directly
    on a non-success outcome -- that would silently reintroduce the exact
    bypass this function exists to close.

    Outcomes:
      * ``success`` -- the row reached DISCOVERY_STATUS_SUCCESS; ``result``
        carries the populator summary.
      * ``failed`` -- the row reached DISCOVERY_STATUS_FAILED; ``reason``
        carries the row's ``error``.
      * ``not_claimed`` -- the row never left DISCOVERY_STATUS_PLANNED
        within one full lease window (``_discovery_lease_seconds()``,
        env ``SYNC_REFERENCE_DISCOVERY_LEASE_SECONDS``, default 300s): no
        worker ever took the lease.
      * ``timeout_running`` -- the row WAS claimed (observed RUNNING or
        RETRYING at least once) but never reached a terminal state within
        ``_max_discovery_lifetime_seconds()`` (env
        ``SYNC_REFERENCE_DISCOVERY_MAX_LIFETIME_SECONDS``, default 3720s)
        of first being observed claimed -- the same bound
        ``NativeReferenceDiscoveryService.claim`` computes its own
        per-attempt deadline from. A pathological full-attempt retry chain
        (``SYNC_REFERENCE_DISCOVERY_MAX_ATTEMPTS`` retries at up to 900s
        backoff each) can in theory still be legitimately in flight past
        this bound; ``timeout_running`` means "check the ledger row
        directly", not "discovery is lost".
    """
    from dev_health_ops.db import get_postgres_session_sync

    run_uuid = uuid.UUID(str(sync_run_id))
    not_claimed_bound = _discovery_lease_seconds()
    running_bound = _max_discovery_lifetime_seconds()
    started = time.monotonic()
    running_deadline: float | None = None

    while True:
        with get_postgres_session_sync() as session:
            ledger = (
                session.query(SyncRunReferenceDiscovery)
                .filter(SyncRunReferenceDiscovery.sync_run_id == run_uuid)
                .one_or_none()
            )
            if ledger is None:
                status, result, error = DISCOVERY_STATUS_PLANNED, None, None
            else:
                status, result, error = ledger.status, ledger.result, ledger.error

        if status == DISCOVERY_STATUS_SUCCESS:
            return {"outcome": "success", "sync_run_id": sync_run_id, "result": result}
        if status == DISCOVERY_STATUS_FAILED:
            return {"outcome": "failed", "sync_run_id": sync_run_id, "reason": error}

        claimed = status in (DISCOVERY_STATUS_RUNNING, DISCOVERY_STATUS_RETRYING)
        now = time.monotonic()
        if claimed and running_deadline is None:
            running_deadline = now + running_bound
        if not claimed and (now - started) >= not_claimed_bound:
            return {"outcome": "not_claimed", "sync_run_id": sync_run_id}
        if running_deadline is not None and now >= running_deadline:
            return {"outcome": "timeout_running", "sync_run_id": sync_run_id}
        time.sleep(poll_interval)


def _ensure_reference_discovery(
    session: Any, run_uuid: uuid.UUID, *, now: datetime
) -> SyncRunReferenceDiscovery:
    ledger = (
        session.query(SyncRunReferenceDiscovery)
        .filter(SyncRunReferenceDiscovery.sync_run_id == run_uuid)
        .one_or_none()
    )
    if ledger is not None:
        return ledger
    run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
    if run is None:
        raise ValueError(f"sync run not found: {run_uuid}")
    ledger = SyncRunReferenceDiscovery(
        sync_run_id=run_uuid,
        org_id=str(run.org_id),
        status=DISCOVERY_STATUS_PLANNED,
        attempts=0,
        available_at=now,
    )
    session.add(ledger)
    session.flush()
    return ledger


def reference_discovery_succeeded(session: Any, run_uuid: uuid.UUID) -> bool:
    return (
        session.query(SyncRunReferenceDiscovery.id)
        .filter(
            SyncRunReferenceDiscovery.sync_run_id == run_uuid,
            SyncRunReferenceDiscovery.status == DISCOVERY_STATUS_SUCCESS,
        )
        .one_or_none()
        is not None
    )


def ensure_reference_discovery_wakeup(
    session: Any, run_uuid: uuid.UUID, *, now: datetime
) -> None:
    ledger = _ensure_reference_discovery(session, run_uuid, now=now)
    available_at = ledger.available_at or now
    upsert_outbox_wakeup(
        session,
        sync_run_id=run_uuid,
        kind=OUTBOX_KIND_DISCOVERY,
        available_at=available_at,
        now=now,
    )


def _discovery_lease_seconds() -> int:
    try:
        return max(1, int(os.getenv("SYNC_REFERENCE_DISCOVERY_LEASE_SECONDS", "300")))
    except ValueError:
        return 300


def _max_discovery_lifetime_seconds() -> int:
    try:
        return max(
            3600,
            int(os.getenv("SYNC_REFERENCE_DISCOVERY_MAX_LIFETIME_SECONDS", "3720")),
        )
    except ValueError:
        return 3720
