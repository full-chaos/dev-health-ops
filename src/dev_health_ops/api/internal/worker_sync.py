"""Authenticated, reference-only bridge for Go sync coordinator workers."""

from __future__ import annotations

import hmac
import os
import uuid
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from dev_health_ops.db import get_postgres_session_sync
from dev_health_ops.models import (
    SyncDispatchOutbox,
    SyncDispatchTransportRoute,
    SyncRun,
)
from dev_health_ops.workers.reference_discovery import run_sync_reference_discovery
from dev_health_ops.workers.sync_units import dispatch_sync_run, finalize_sync_run
from dev_health_ops.workers.team_autoimport import run_post_sync_team_autoimport

router = APIRouter(prefix="/api/internal/worker-sync", include_in_schema=False)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SyncCoordinatorReference(_StrictModel):
    """A bounded River reference; the database remains the source of truth."""

    organization_id: uuid.UUID
    sync_run_id: uuid.UUID
    outbox_id: uuid.UUID
    route_generation: int = Field(ge=1)


class TeamAutoImportReference(_StrictModel):
    organization_id: uuid.UUID
    sync_run_id: uuid.UUID


def _authorize(authorization: Annotated[str | None, Header()] = None) -> None:
    expected = os.environ.get("WORKER_OPERATIONAL_BRIDGE_TOKEN", "")
    supplied = authorization or ""
    if (
        not expected
        or not supplied.startswith("Bearer ")
        or not hmac.compare_digest(supplied[7:], expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _result(result: object, *, accepted: frozenset[str]) -> dict[str, str]:
    if not isinstance(result, dict):
        raise HTTPException(
            status_code=502, detail="Sync coordinator result unavailable"
        )
    status = str(result.get("status", "unknown"))
    if status not in accepted:
        raise HTTPException(
            status_code=422, detail="Sync coordinator delivery rejected"
        )
    return {"status": status}


def _current_river_reference(reference: SyncCoordinatorReference, *, kind: str) -> bool:
    """Accept only the exact durable River delivery that created this job.

    A River retry may arrive after a route pause, rollback, or a later
    generation became active. The job envelope alone is not authoritative, so
    stale work is acknowledged without calling the durable coordinator.
    """

    with get_postgres_session_sync() as session:
        outbox = (
            session.query(SyncDispatchOutbox)
            .filter(
                SyncDispatchOutbox.id == reference.outbox_id,
                SyncDispatchOutbox.sync_run_id == reference.sync_run_id,
                SyncDispatchOutbox.org_id == str(reference.organization_id),
                SyncDispatchOutbox.kind == kind,
                SyncDispatchOutbox.status == "dispatched",
                SyncDispatchOutbox.dispatched_transport == "river",
                SyncDispatchOutbox.dispatched_route_generation
                == reference.route_generation,
            )
            .one_or_none()
        )
        route = (
            session.query(SyncDispatchTransportRoute)
            .filter(
                SyncDispatchTransportRoute.kind == kind,
                SyncDispatchTransportRoute.transport == "river",
                SyncDispatchTransportRoute.paused.is_(False),
                SyncDispatchTransportRoute.generation == reference.route_generation,
            )
            .one_or_none()
        )
    return outbox is not None and route is not None


def _current_sync_run_reference(reference: TeamAutoImportReference) -> bool:
    """Reject a trusted bridge request whose run belongs to another tenant."""

    with get_postgres_session_sync() as session:
        run = (
            session.query(SyncRun.id)
            .filter(
                SyncRun.id == reference.sync_run_id,
                SyncRun.org_id == str(reference.organization_id),
            )
            .one_or_none()
        )
    return run is not None


@router.post("/dispatch", dependencies=[])
async def dispatch_reference(
    reference: SyncCoordinatorReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    _authorize(authorization)
    if not _current_river_reference(reference, kind="dispatch_sync_run"):
        return {"status": "stale"}
    # The function loads its SyncRun, units, reference-discovery ledger, budget
    # state, and durable wakeups from PostgreSQL. The River message contains no
    # executable command, credentials, or provider payload.
    result = await run_in_threadpool(dispatch_sync_run.run, str(reference.sync_run_id))
    return _result(
        result,
        accepted=frozenset(
            {
                "missing",
                "feature_disabled",
                "blocked_on_reference_discovery",
                "denied",
                "denied_active",
                "dispatched",
                "noop",
                "waiting_inflight",
                "deferred",
            }
        ),
    )


@router.post("/finalize", dependencies=[])
async def finalize_reference(
    reference: SyncCoordinatorReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    _authorize(authorization)
    if not _current_river_reference(reference, kind="finalize_sync_run"):
        return {"status": "stale"}
    result = await run_in_threadpool(finalize_sync_run.run, str(reference.sync_run_id))
    return _result(
        result,
        accepted=frozenset({"missing", "pending", "already_dispatched", "finalized"}),
    )


@router.post("/reference-discovery", dependencies=[])
async def reference_discovery_reference(
    reference: SyncCoordinatorReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    _authorize(authorization)
    if not _current_river_reference(reference, kind="reference_discovery"):
        return {"status": "stale"}
    result = await run_in_threadpool(
        run_sync_reference_discovery.run, str(reference.sync_run_id)
    )
    return _result(
        result,
        accepted=frozenset(
            {"feature_disabled", "success", "skipped", "retrying", "failed"}
        ),
    )


@router.post("/team-autoimport", dependencies=[])
async def team_autoimport_reference(
    reference: TeamAutoImportReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    _authorize(authorization)
    if not _current_sync_run_reference(reference):
        return {"status": "stale"}
    result = await run_in_threadpool(
        run_post_sync_team_autoimport.run, str(reference.sync_run_id)
    )
    return _result(
        result,
        accepted=frozenset({"skipped", "dispatched"}),
    )
