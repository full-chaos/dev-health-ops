"""Authenticated, reference-only bridge for Go sync coordinator workers."""

from __future__ import annotations

import hmac
import os
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from dev_health_ops.db import get_postgres_session_sync
from dev_health_ops.models import (
    SyncDispatchOutbox,
    SyncDispatchTransportRoute,
    SyncRun,
    SyncRunUnit,
)
from dev_health_ops.sync.budget_guard import batch_estimate_provider_budget_for_units
from dev_health_ops.workers.reference_discovery import (
    run_reference_discovery_populate_for_sync_run,
    run_sync_reference_discovery,
)
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


class DispatchBudgetEstimateReference(_StrictModel):
    """Identifiers only, matching every other bridge reference in this
    module -- no credential material, no provider payload, ever crosses
    this boundary (CHAOS-4175). unit_ids is bounded to keep one dispatch
    pass's batch from becoming an unbounded credential-decryption fan-out;
    500 comfortably covers SYNC_UNIT_CONCURRENCY_PER_BUCKET's realistic
    worst case across every bucket in one run.
    """

    organization_id: uuid.UUID
    sync_run_id: uuid.UUID
    unit_ids: list[uuid.UUID] = Field(min_length=1, max_length=500)


class BudgetEstimateBucketPayload(_StrictModel):
    provider: str
    org_id: str
    host: str
    credential_fingerprint: str
    dimension: str


class BudgetEstimatePayload(_StrictModel):
    bucket: BudgetEstimateBucketPayload
    estimated_units: int
    confidence: str
    route_family: str
    notes: list[str] = Field(default_factory=list)


class DispatchBudgetEstimateResponse(_StrictModel):
    """The closed BudgetEstimate schema, keyed by unit id (as a string --
    JSON object keys are always strings; the Go client parses each key back
    to a UUID). A unit id present in the request but ABSENT from this dict's
    keys, or mapped to an empty list, both mean "no budget constraint for
    this unit" -- estimate_provider_budget legitimately returns an empty
    tuple for an unrecognized provider, and
    batch_estimate_provider_budget_for_units degrades a bootstrap/estimate
    failure to the same empty shape rather than failing the whole batch
    (see that function's docstring). The Go caller must treat both the
    same way Python's own enforce_run does: no estimate to check against
    any budget bucket, not a hard failure.
    """

    estimates: dict[str, list[BudgetEstimatePayload]]


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


def _units_belong_to_run(
    session: Any,
    sync_run_id: uuid.UUID,
    organization_id: uuid.UUID,
    unit_ids: list[uuid.UUID],
) -> bool:
    """Reject any unit id NOT owned by this (tenant-fenced) run.

    SyncTaskBootstrap.load itself only ever filters on unit.id, not
    sync_run_id or org_id -- there is no Python precedent for a batched,
    caller-supplied unit-id-list endpoint like this one, so this endpoint
    must independently prove every requested id actually belongs to this
    run BEFORE bootstrapping (and decrypting credentials for) any of them.
    Rejecting the WHOLE batch on ANY mismatch, rather than silently
    dropping the offending id, is deliberate: a mismatched id here can only
    mean a Go-side bug (it should never legitimately gather a unit id
    outside the run it is dispatching), and that must surface loudly, not
    disappear into an empty-estimate result indistinguishable from a
    normal "unrecognized provider" no-op.
    """

    matched = (
        session.query(SyncRunUnit.id)
        .filter(
            SyncRunUnit.id.in_(unit_ids),
            SyncRunUnit.sync_run_id == sync_run_id,
            SyncRunUnit.org_id == str(organization_id),
        )
        .count()
    )
    return matched == len(set(unit_ids))


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


@router.post("/reference-discovery-populate", dependencies=[])
async def reference_discovery_populate_reference(
    reference: TeamAutoImportReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    """Wraps run_reference_discovery_populate_for_sync_run EXACTLY.

    This is the ONE narrow, synchronous bridge call CHAOS-4175's native
    Go reference-discovery gate makes (ruling widened 2026-08-24): the
    request carries organization_id/sync_run_id only, and the response is
    the populator's summary dict only. Credential resolution -- Fernet
    decryption, PagerDuty OAuth token rotation, the CHAOS-2755 stamped-
    auth freeze -- happens entirely inside
    run_reference_discovery_populate_for_sync_run, on this side of the
    boundary; TeamAutoImportReference's own field set (see below) is the
    contract that no secret material can cross it. See
    test_worker_sync_bridge.py's identifiers-only pin for the enforced
    shape.
    """
    _authorize(authorization)
    if not _current_sync_run_reference(reference):
        raise HTTPException(status_code=409, detail="Sync run reference is stale")
    return await run_in_threadpool(
        run_reference_discovery_populate_for_sync_run, str(reference.sync_run_id)
    )


def _dispatch_budget_estimate(
    reference: DispatchBudgetEstimateReference,
) -> DispatchBudgetEstimateResponse:
    with get_postgres_session_sync() as session:
        if not _units_belong_to_run(
            session,
            reference.sync_run_id,
            reference.organization_id,
            reference.unit_ids,
        ):
            raise HTTPException(
                status_code=409,
                detail="One or more units do not belong to this sync run",
            )
        estimates_by_unit = batch_estimate_provider_budget_for_units(
            session,
            str(reference.sync_run_id),
            (str(unit_id) for unit_id in reference.unit_ids),
        )
    return DispatchBudgetEstimateResponse(
        estimates={
            unit_id: [
                BudgetEstimatePayload(
                    bucket=BudgetEstimateBucketPayload(**estimate.bucket.to_dict()),
                    estimated_units=estimate.estimated_units,
                    confidence=estimate.confidence,
                    route_family=estimate.route_family,
                    notes=list(estimate.notes),
                )
                for estimate in estimates
            ]
            for unit_id, estimates in estimates_by_unit.items()
        }
    )


@router.post(
    "/dispatch-budget-estimate",
    response_model=DispatchBudgetEstimateResponse,
    dependencies=[],
)
async def dispatch_budget_estimate_reference(
    reference: DispatchBudgetEstimateReference,
    authorization: Annotated[str | None, Header()] = None,
) -> DispatchBudgetEstimateResponse:
    """Wraps batch_estimate_provider_budget_for_units EXACTLY.

    This is the ONE narrow, synchronous bridge call CHAOS-4175's native Go
    BudgetGuard port makes (ruled 2026-08-24): the request carries
    organization_id/sync_run_id/unit_ids only, and the response is the
    closed BudgetEstimate schema only -- no credential material, no
    provider payload, either direction. SyncTaskBootstrap.load's Fernet
    decryption and the six per-provider estimator classes
    (estimate_provider_budget) run entirely inside
    batch_estimate_provider_budget_for_units, on this side of the
    boundary. See test_worker_sync_bridge.py's identifiers-only /
    closed-response pins for the enforced shape on both directions.

    Every requested unit id is verified to belong to sync_run_id (tenant-
    fenced, see _units_belong_to_run) before any credential is decrypted --
    unlike the other bridge endpoints in this module, this one's request
    body names a whole LIST of domain objects, so the usual single-run
    tenant check is not enough on its own.
    """
    _authorize(authorization)
    if not _current_sync_run_reference(
        TeamAutoImportReference(
            organization_id=reference.organization_id, sync_run_id=reference.sync_run_id
        )
    ):
        raise HTTPException(status_code=409, detail="Sync run reference is stale")
    return await run_in_threadpool(_dispatch_budget_estimate, reference)
