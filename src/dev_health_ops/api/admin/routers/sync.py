from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    JOB_RUN_STATUS_LABELS,
    BackfillRequest,
    JobRunResponse,
    SyncConfigCreate,
    SyncConfigResponse,
    SyncConfigUpdate,
)
from dev_health_ops.api.services.licensing import TierLimitService
from dev_health_ops.api.services.settings import SyncConfigurationService
from dev_health_ops.models.settings import JobRun, ScheduledJob

from .common import get_session

router = APIRouter()

# Canonical mapping of provider → supported sync targets.
# Jira/Linear only support work-items; Git/CI/CD come from code hosts.
PROVIDER_SYNC_TARGETS: dict[str, list[str]] = {
    "github": ["git", "prs", "cicd", "deployments", "incidents", "work-items"],
    "gitlab": ["git", "prs", "cicd", "deployments", "incidents", "work-items"],
    "jira": ["work-items"],
    "linear": ["work-items"],
}


@router.get("/sync-targets")
async def get_provider_sync_targets() -> dict[str, list[str]]:
    return PROVIDER_SYNC_TARGETS


@router.get("/sync-configs", response_model=list[SyncConfigResponse])
async def list_sync_configs(
    active_only: bool = False,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[SyncConfigResponse]:
    svc = SyncConfigurationService(session, org_id)
    configs = await svc.list_all(active_only=active_only)
    return [_sync_config_to_response(c) for c in configs]


@router.post("/sync-configs", response_model=SyncConfigResponse, status_code=201)
async def create_sync_config(
    payload: SyncConfigCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.create(
        name=payload.name,
        provider=payload.provider,
        sync_targets=payload.sync_targets,
        sync_options=payload.sync_options,
        credential_id=payload.credential_id,
    )
    return _sync_config_to_response(config)


def _sync_config_to_response(c) -> SyncConfigResponse:
    return SyncConfigResponse(
        id=str(c.id),
        name=c.name,
        provider=c.provider,
        credential_id=str(c.credential_id) if c.credential_id else None,
        sync_targets=c.sync_targets,
        sync_options=c.sync_options,
        is_active=c.is_active,
        last_sync_at=c.last_sync_at,
        last_sync_success=c.last_sync_success,
        last_sync_error=c.last_sync_error,
        created_at=c.created_at,
        updated_at=c.updated_at,
    )


@router.get("/sync-configs/{config_id}", response_model=SyncConfigResponse)
async def get_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    return _sync_config_to_response(config)


@router.patch("/sync-configs/{config_id}", response_model=SyncConfigResponse)
async def update_sync_config(
    config_id: str,
    payload: SyncConfigUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    updated = await svc.update(
        name=config.name,
        sync_targets=payload.sync_targets,
        sync_options=payload.sync_options,
        is_active=payload.is_active,
    )
    return _sync_config_to_response(updated)


@router.delete("/sync-configs/{config_id}", status_code=204)
async def delete_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> None:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    await svc.delete(config.name)


@router.post("/sync-configs/{config_id}/trigger", status_code=202)
async def trigger_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    try:
        from dev_health_ops.workers.sync_tasks import run_sync_config

        result = run_sync_config.delay(
            config_id=str(config.id),
            org_id=org_id,
            triggered_by="manual",
        )
        return {
            "status": "triggered",
            "config_id": str(config.id),
            "task_id": result.id,
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Task queue unavailable: {e}")


@router.post("/sync-configs/{config_id}/backfill", status_code=202)
async def trigger_sync_config_backfill(
    config_id: str,
    payload: BackfillRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    requested_days = (payload.before - payload.since).days

    def _check_backfill_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_backfill_limit(uuid.UUID(org_id), requested_days)

    allowed, reason = await session.run_sync(_check_backfill_limit)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason or "Backfill not allowed")

    from dev_health_ops.backfill.chunker import chunk_date_range
    from dev_health_ops.models.backfill import BackfillJob as BackfillJobModel

    windows = chunk_date_range(since=payload.since, before=payload.before, chunk_days=7)
    backfill_job = BackfillJobModel(
        org_id=org_id,
        sync_config_id=uuid.UUID(config_id),
        status="pending",
        since_date=payload.since,
        before_date=payload.before,
        total_chunks=len(windows),
    )
    session.add(backfill_job)
    await session.flush()
    backfill_job_id = str(backfill_job.id)

    try:
        from dev_health_ops.workers.sync_tasks import run_backfill

        result = run_backfill.delay(
            sync_config_id=str(config.id),
            since=payload.since.isoformat(),
            before=payload.before.isoformat(),
            org_id=org_id,
            backfill_job_id=backfill_job_id,
        )
        backfill_job.celery_task_id = result.id
        await session.flush()
        return {
            "status": "accepted",
            "config_id": str(config.id),
            "task_id": result.id,
            "backfill_job_id": backfill_job_id,
            "since": payload.since.isoformat(),
            "before": payload.before.isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Task queue unavailable: {e}")


@router.get("/sync-configs/{config_id}/jobs", response_model=list[JobRunResponse])
async def list_sync_config_jobs(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[JobRunResponse]:
    svc = SyncConfigurationService(session, org_id)
    existing = await svc.get_by_id(config_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    job_stmt = select(ScheduledJob.id).where(
        ScheduledJob.org_id == org_id,
        ScheduledJob.sync_config_id == uuid.UUID(config_id),
        ScheduledJob.job_type == "sync",
    )
    job_result = await session.execute(job_stmt)
    job_ids = list(job_result.scalars().all())

    if not job_ids:
        return []

    runs_stmt = (
        select(JobRun)
        .where(JobRun.job_id.in_(job_ids))
        .order_by(JobRun.created_at.desc())
        .limit(50)
    )
    runs_result = await session.execute(runs_stmt)
    runs = list(runs_result.scalars().all())

    return [
        JobRunResponse(
            id=str(run.id),
            job_id=str(run.job_id),
            status=JOB_RUN_STATUS_LABELS.get(run.status, "unknown"),
            started_at=run.started_at,
            completed_at=run.completed_at,
            duration_seconds=run.duration_seconds,
            result=run.result,
            error=run.error,
            triggered_by=run.triggered_by,
            created_at=run.created_at,
        )
        for run in runs
    ]


@router.get("/backfill-jobs")
async def list_backfill_jobs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.schemas.backfill import (
        BackfillJobListResponse,
        BackfillJobResponse,
    )
    from dev_health_ops.api.services.backfill import BackfillJobService

    svc = BackfillJobService(session, org_id)
    jobs, total = await svc.list_jobs(limit=limit, offset=offset)
    return BackfillJobListResponse(
        items=[
            BackfillJobResponse(
                id=str(j.id),
                sync_config_id=str(j.sync_config_id),
                status=j.status,
                since_date=j.since_date,
                before_date=j.before_date,
                total_chunks=j.total_chunks,
                completed_chunks=j.completed_chunks,
                failed_chunks=j.failed_chunks,
                progress_pct=(j.completed_chunks / j.total_chunks * 100)
                if j.total_chunks > 0
                else 0.0,
                error_message=j.error_message,
                started_at=j.started_at,
                completed_at=j.completed_at,
                created_at=j.created_at,
            )
            for j in jobs
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/backfill-jobs/{job_id}")
async def get_backfill_job(
    job_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.schemas.backfill import BackfillJobResponse
    from dev_health_ops.api.services.backfill import BackfillJobService

    svc = BackfillJobService(session, org_id)
    job = await svc.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    return BackfillJobResponse(
        id=str(job.id),
        sync_config_id=str(job.sync_config_id),
        status=job.status,
        since_date=job.since_date,
        before_date=job.before_date,
        total_chunks=job.total_chunks,
        completed_chunks=job.completed_chunks,
        failed_chunks=job.failed_chunks,
        progress_pct=(job.completed_chunks / job.total_chunks * 100)
        if job.total_chunks > 0
        else 0.0,
        error_message=job.error_message,
        started_at=job.started_at,
        completed_at=job.completed_at,
        created_at=job.created_at,
    )
