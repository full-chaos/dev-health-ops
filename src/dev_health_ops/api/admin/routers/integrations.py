"""Admin router for integrations, sources, datasets, and sync runs."""

from __future__ import annotations

import logging
import uuid as _uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas.integrations import (
    BackfillTriggerRequest,
    DiscoverResponse,
    IntegrationCreate,
    IntegrationDatasetBatchUpdate,
    IntegrationDatasetResponse,
    IntegrationResponse,
    IntegrationSourceResponse,
    IntegrationSourceUpdate,
    IntegrationUpdate,
    SyncRunResponse,
    SyncRunUnitResponse,
    SyncRunUnitSummary,
    SyncTriggerRequest,
    SyncTriggerResponse,
)
from dev_health_ops.api.services.integrations import (
    IntegrationDatasetService,
    IntegrationService,
    IntegrationSourceService,
    SyncRunService,
)
from dev_health_ops.sync.discovery import discover_sources_for_integration
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from dev_health_ops.sync.trigger_routing import map_sync_mode
from dev_health_ops.workers.sync_units import dispatch_sync_run

from .common import get_session

logger = logging.getLogger(__name__)


def _safe_log_value(value: object, *, max_length: int = 500) -> str:
    text = str(value)
    sanitized = "".join(
        char if char.isprintable() and char not in {"\n", "\r", "\t"} else " "
        for char in text
    )
    return sanitized[:max_length]


router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _integration_to_response(integration: object) -> IntegrationResponse:
    return IntegrationResponse.model_validate(
        {
            "id": str(getattr(integration, "id")),
            "org_id": str(getattr(integration, "org_id")),
            "provider": str(getattr(integration, "provider")),
            "credential_id": (
                str(getattr(integration, "credential_id"))
                if getattr(integration, "credential_id") is not None
                else None
            ),
            "name": str(getattr(integration, "name")),
            "config": dict(getattr(integration, "config") or {}),
            "is_active": bool(getattr(integration, "is_active")),
            "schedule_cron": getattr(integration, "schedule_cron"),
            "timezone": getattr(integration, "timezone"),
            "created_at": getattr(integration, "created_at"),
            "updated_at": getattr(integration, "updated_at"),
        }
    )


def _source_to_response(source: object) -> IntegrationSourceResponse:
    return IntegrationSourceResponse.model_validate(
        {
            "id": str(getattr(source, "id")),
            "org_id": str(getattr(source, "org_id")),
            "integration_id": str(getattr(source, "integration_id")),
            "provider": str(getattr(source, "provider")),
            "source_type": str(getattr(source, "source_type")),
            "external_id": str(getattr(source, "external_id")),
            "name": str(getattr(source, "name")),
            "full_name": str(getattr(source, "full_name")),
            "metadata": dict(getattr(source, "metadata_") or {}),
            "is_enabled": bool(getattr(source, "is_enabled")),
            "discovered_at": getattr(source, "discovered_at"),
            "last_seen_at": getattr(source, "last_seen_at"),
            "last_sync_at": getattr(source, "last_sync_at"),
            "last_sync_success": getattr(source, "last_sync_success"),
            "last_sync_error": getattr(source, "last_sync_error"),
        }
    )


def _dataset_to_response(dataset: object) -> IntegrationDatasetResponse:
    return IntegrationDatasetResponse.model_validate(
        {
            "id": str(getattr(dataset, "id")),
            "org_id": str(getattr(dataset, "org_id")),
            "integration_id": str(getattr(dataset, "integration_id")),
            "dataset_key": str(getattr(dataset, "dataset_key")),
            "is_enabled": bool(getattr(dataset, "is_enabled")),
            "options": dict(getattr(dataset, "options") or {}),
        }
    )


def _sync_run_to_response(run: object) -> SyncRunResponse:
    return SyncRunResponse.model_validate(
        {
            "id": str(getattr(run, "id")),
            "org_id": str(getattr(run, "org_id")),
            "integration_id": str(getattr(run, "integration_id")),
            "triggered_by": str(getattr(run, "triggered_by")),
            "mode": str(getattr(run, "mode")),
            "status": str(getattr(run, "status")),
            "total_units": int(getattr(run, "total_units")),
            "completed_units": int(getattr(run, "completed_units")),
            "failed_units": int(getattr(run, "failed_units")),
            "started_at": getattr(run, "started_at"),
            "completed_at": getattr(run, "completed_at"),
            "result": getattr(run, "result"),
            "error": getattr(run, "error"),
            "created_at": getattr(run, "created_at"),
        }
    )


def _unit_to_response(unit: object) -> SyncRunUnitResponse:
    return SyncRunUnitResponse.model_validate(
        {
            "id": str(getattr(unit, "id")),
            "org_id": str(getattr(unit, "org_id")),
            "sync_run_id": str(getattr(unit, "sync_run_id")),
            "integration_id": str(getattr(unit, "integration_id")),
            "source_id": str(getattr(unit, "source_id")),
            "provider": str(getattr(unit, "provider")),
            "dataset_key": str(getattr(unit, "dataset_key")),
            "cost_class": str(getattr(unit, "cost_class")),
            "mode": str(getattr(unit, "mode")),
            "since_at": getattr(unit, "since_at"),
            "before_at": getattr(unit, "before_at"),
            "status": str(getattr(unit, "status")),
            "attempts": int(getattr(unit, "attempts")),
            "duration_seconds": getattr(unit, "duration_seconds"),
            "error": getattr(unit, "error"),
            "result": getattr(unit, "result"),
            "created_at": getattr(unit, "created_at"),
            "updated_at": getattr(unit, "updated_at"),
        }
    )


# ---------------------------------------------------------------------------
# Integration CRUD
# ---------------------------------------------------------------------------


@router.get("/integrations", response_model=list[IntegrationResponse])
async def list_integrations(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[IntegrationResponse]:
    svc = IntegrationService(session, org_id)
    integrations = await svc.list_all()
    return [_integration_to_response(i) for i in integrations]


@router.post("/integrations", response_model=IntegrationResponse, status_code=201)
async def create_integration(
    payload: IntegrationCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IntegrationResponse:
    svc = IntegrationService(session, org_id)
    try:
        integration = await svc.create(
            name=payload.name,
            provider=payload.provider,
            credential_id=payload.credential_id,
            config=payload.config,
            is_active=payload.is_active,
            schedule_cron=payload.schedule_cron,
            timezone=payload.timezone,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _integration_to_response(integration)


@router.get("/integrations/{integration_id}", response_model=IntegrationResponse)
async def get_integration(
    integration_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IntegrationResponse:
    svc = IntegrationService(session, org_id)
    integration = await svc.get_by_id(integration_id)
    if integration is None:
        raise HTTPException(status_code=404, detail="Integration not found")
    return _integration_to_response(integration)


@router.patch("/integrations/{integration_id}", response_model=IntegrationResponse)
async def update_integration(
    integration_id: str,
    payload: IntegrationUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IntegrationResponse:
    svc = IntegrationService(session, org_id)
    integration = await svc.get_by_id(integration_id)
    if integration is None:
        raise HTTPException(status_code=404, detail="Integration not found")
    try:
        updated = await svc.update(
            integration,
            name=payload.name,
            credential_id=payload.credential_id,
            config=payload.config,
            is_active=payload.is_active,
            schedule_cron=payload.schedule_cron,
            timezone=payload.timezone,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _integration_to_response(updated)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


@router.post(
    "/integrations/{integration_id}/discover",
    response_model=DiscoverResponse,
    status_code=202,
)
async def discover_integration_sources(
    integration_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> DiscoverResponse:
    """Discover provider sources for an integration and upsert them."""
    svc = IntegrationService(session, org_id)
    integration = await svc.get_by_id(integration_id)
    if integration is None:
        raise HTTPException(status_code=404, detail="Integration not found")

    try:
        # discovery.py uses a synchronous session internally
        sources = await session.run_sync(
            lambda sync_session: discover_sources_for_integration(
                sync_session,
                _uuid.UUID(integration_id),
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception(
            "integration_discovery.failed",
            extra={
                "integration_id": _safe_log_value(integration_id),
                "error": _safe_log_value(exc),
                "error_type": type(exc).__name__,
            },
        )
        raise HTTPException(status_code=503, detail=f"Discovery failed: {exc}")

    return DiscoverResponse(
        integration_id=integration_id,
        discovered=len(sources),
        sources=[_source_to_response(s) for s in sources],
    )


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


@router.get(
    "/integrations/{integration_id}/sources",
    response_model=list[IntegrationSourceResponse],
)
async def list_integration_sources(
    integration_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[IntegrationSourceResponse]:
    int_svc = IntegrationService(session, org_id)
    if await int_svc.get_by_id(integration_id) is None:
        raise HTTPException(status_code=404, detail="Integration not found")

    svc = IntegrationSourceService(session, org_id)
    sources = await svc.list_for_integration(integration_id)
    return [_source_to_response(s) for s in sources]


@router.patch(
    "/integrations/{integration_id}/sources/{source_id}",
    response_model=IntegrationSourceResponse,
)
async def update_integration_source(
    integration_id: str,
    source_id: str,
    payload: IntegrationSourceUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IntegrationSourceResponse:
    int_svc = IntegrationService(session, org_id)
    if await int_svc.get_by_id(integration_id) is None:
        raise HTTPException(status_code=404, detail="Integration not found")

    svc = IntegrationSourceService(session, org_id)
    source = await svc.get_by_id(integration_id, source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")

    updated = await svc.set_enabled(source, payload.is_enabled)
    return _source_to_response(updated)


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


@router.get(
    "/integrations/{integration_id}/datasets",
    response_model=list[IntegrationDatasetResponse],
)
async def list_integration_datasets(
    integration_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[IntegrationDatasetResponse]:
    int_svc = IntegrationService(session, org_id)
    if await int_svc.get_by_id(integration_id) is None:
        raise HTTPException(status_code=404, detail="Integration not found")

    svc = IntegrationDatasetService(session, org_id)
    datasets = await svc.list_for_integration(integration_id)
    return [_dataset_to_response(d) for d in datasets]


@router.patch(
    "/integrations/{integration_id}/datasets",
    response_model=list[IntegrationDatasetResponse],
)
async def update_integration_datasets(
    integration_id: str,
    payload: IntegrationDatasetBatchUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[IntegrationDatasetResponse]:
    """Enable or disable dataset rows by dataset_key."""
    int_svc = IntegrationService(session, org_id)
    if await int_svc.get_by_id(integration_id) is None:
        raise HTTPException(status_code=404, detail="Integration not found")

    svc = IntegrationDatasetService(session, org_id)
    updated = []
    for item in payload.datasets:
        dataset = await svc.get_by_key(integration_id, item.dataset_key)
        if dataset is None:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{item.dataset_key}' not found",
            )
        updated.append(await svc.set_enabled(dataset, item.is_enabled))
    return [_dataset_to_response(d) for d in updated]


# ---------------------------------------------------------------------------
# Sync / Backfill trigger
# ---------------------------------------------------------------------------


@router.post(
    "/integrations/{integration_id}/sync",
    response_model=SyncTriggerResponse,
    status_code=202,
)
async def trigger_integration_sync(
    integration_id: str,
    payload: SyncTriggerRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncTriggerResponse:
    """Plan a sync run and dispatch it. Pass full_resync=true for a full-resync."""
    int_svc = IntegrationService(session, org_id)
    if await int_svc.get_by_id(integration_id) is None:
        raise HTTPException(status_code=404, detail="Integration not found")

    request = SyncPlanRequest(
        integration_id=integration_id,
        org_id=org_id,
        mode=map_sync_mode("full_resync") if payload.full_resync else "incremental",
        triggered_by="admin-api",
        source_ids=tuple(payload.source_ids)
        if payload.source_ids is not None
        else None,
        dataset_keys=tuple(payload.dataset_keys)
        if payload.dataset_keys is not None
        else None,
    )

    try:
        plan = await session.run_sync(
            lambda sync_session: plan_sync_run(sync_session, request)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Commit the planned run BEFORE enqueueing dispatch so the Celery worker
    # (a separate DB session) can see it; otherwise a fast worker returns
    # "missing" and the run is stranded as planned.
    await session.commit()

    try:
        getattr(dispatch_sync_run, "apply_async")(
            args=(plan.sync_run_id,),
            queue="sync",
        )
    except Exception as exc:
        logger.warning(
            "integration_sync.dispatch_fastpath_failed",
            extra={
                "integration_id": _safe_log_value(integration_id),
                "sync_run_id": _safe_log_value(plan.sync_run_id),
                "error": _safe_log_value(exc),
                "error_type": type(exc).__name__,
            },
        )

    return SyncTriggerResponse(
        status="accepted",
        integration_id=integration_id,
        sync_run_id=plan.sync_run_id,
        total_units=plan.total_units,
    )


@router.post(
    "/integrations/{integration_id}/backfill",
    response_model=SyncTriggerResponse,
    status_code=202,
)
async def trigger_integration_backfill(
    integration_id: str,
    payload: BackfillTriggerRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncTriggerResponse:
    """Plan a backfill sync run and dispatch it."""
    int_svc = IntegrationService(session, org_id)
    if await int_svc.get_by_id(integration_id) is None:
        raise HTTPException(status_code=404, detail="Integration not found")

    request = SyncPlanRequest(
        integration_id=integration_id,
        org_id=org_id,
        mode="backfill",
        triggered_by="admin-api",
        source_ids=tuple(payload.source_ids)
        if payload.source_ids is not None
        else None,
        dataset_keys=tuple(payload.dataset_keys)
        if payload.dataset_keys is not None
        else None,
        since=payload.since,
        before=payload.before,
    )

    try:
        plan = await session.run_sync(
            lambda sync_session: plan_sync_run(sync_session, request)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Commit the planned backfill run before dispatch (see /sync rationale).
    await session.commit()

    try:
        getattr(dispatch_sync_run, "apply_async")(
            args=(plan.sync_run_id,),
            queue="sync",
        )
    except Exception as exc:
        logger.warning(
            "integration_backfill.dispatch_fastpath_failed",
            extra={
                "integration_id": _safe_log_value(integration_id),
                "sync_run_id": _safe_log_value(plan.sync_run_id),
                "error": _safe_log_value(exc),
                "error_type": type(exc).__name__,
            },
        )

    return SyncTriggerResponse(
        status="accepted",
        integration_id=integration_id,
        sync_run_id=plan.sync_run_id,
        total_units=plan.total_units,
    )


# ---------------------------------------------------------------------------
# Sync run status
# ---------------------------------------------------------------------------


@router.get("/sync-runs/{run_id}", response_model=SyncRunResponse)
async def get_sync_run(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncRunResponse:
    svc = SyncRunService(session, org_id)
    run = await svc.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Sync run not found")
    return _sync_run_to_response(run)


@router.get("/sync-runs/{run_id}/units", response_model=SyncRunUnitSummary)
async def get_sync_run_units(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    limit: int = 200,
) -> SyncRunUnitSummary:
    svc = SyncRunService(session, org_id)
    run = await svc.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Sync run not found")

    units = await svc.list_units(run_id)
    rollups = SyncRunService.build_unit_rollups(units)
    total_units = len(units)

    return SyncRunUnitSummary(
        by_status=rollups["by_status"],
        by_source=rollups["by_source"],
        by_dataset=rollups["by_dataset"],
        by_cost_class=rollups["by_cost_class"],
        slowest_unit_ids=rollups["slowest_unit_ids"],
        failed_unit_ids=rollups["failed_unit_ids"],
        partial_failure_summary=rollups["partial_failure_summary"],
        failed_unit_count=rollups["failed_unit_count"],
        unit_count=total_units,
        units=[_unit_to_response(u) for u in units[:limit]],
    )
