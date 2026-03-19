from __future__ import annotations

import uuid

from croniter import croniter as Croniter
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    JOB_RUN_STATUS_LABELS,
    BackfillRequest,
    JobRunResponse,
    SyncConfigBatchCreate,
    SyncConfigBatchResponse,
    SyncConfigCreate,
    SyncConfigResponse,
    SyncConfigUpdate,
)
from dev_health_ops.api.services.licensing import TierLimitService
from dev_health_ops.api.services.settings import SyncConfigurationService
from dev_health_ops.models.settings import JobRun, ScheduledJob, SyncConfiguration

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
    parent_only: bool = False,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[SyncConfigResponse]:
    svc = SyncConfigurationService(session, org_id)
    configs = await svc.list_all(active_only=active_only)
    if parent_only:
        configs = [c for c in configs if c.parent_id is None]

    # Build children count map without lazy-loading relationships
    from sqlalchemy import func, select
    children_counts: dict[str, int] = {}
    parent_ids = [c.id for c in configs if c.parent_id is None]
    if parent_ids:
        stmt = (
            select(
                SyncConfiguration.parent_id,
                func.count(SyncConfiguration.id),
            )
            .where(SyncConfiguration.parent_id.in_(parent_ids))
            .group_by(SyncConfiguration.parent_id)
        )
        rows = (await session.execute(stmt)).all()
        children_counts = {str(pid): cnt for pid, cnt in rows}

    results = []
    for c in configs:
        cc = children_counts.get(str(c.id))
        results.append(_sync_config_to_response(c, children_count=cc))
    return results


@router.post(
    "/sync-configs/batch", response_model=SyncConfigBatchResponse, status_code=201
)
async def batch_create_sync_configs(
    payload: SyncConfigBatchCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigBatchResponse:
    """Create a parent sync config + one child per repo."""
    svc = SyncConfigurationService(session, org_id)

    # Enforce repo limit (existing + new repos)
    existing_configs = await svc.list_all(active_only=True)
    current_count = len(existing_configs)
    new_count = len(payload.repos)

    def _check_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_repo_limit(uuid.UUID(org_id), current_count + new_count)

    allowed, reason = await session.run_sync(_check_limit)
    if not allowed:
        raise HTTPException(
            status_code=403,
            detail=reason or f"Repo limit exceeded (adding {new_count} repos)",
        )

    # Create parent config (template — not synced directly)
    parent_options = dict(payload.sync_options)
    parent_options.pop("repo", None)  # parent has no single repo
    parent = SyncConfiguration(
        name=payload.name,
        provider=payload.provider,
        org_id=org_id,
        credential_id=uuid.UUID(payload.credential_id)
        if payload.credential_id
        else None,
        sync_targets=payload.sync_targets,
        sync_options=parent_options,
        is_active=False,  # parent is a template, children are the active jobs
    )
    session.add(parent)
    await session.flush()  # need parent.id for children

    # Create child configs (one per repo)
    children = []
    for repo_name in payload.repos:
        child_options = dict(parent_options)
        child_options["repo"] = repo_name
        if payload.initial_sync_depth is not None:
            child_options["initial_sync_depth"] = payload.initial_sync_depth
        if payload.schedule_cron is not None:
            child_options["schedule_cron"] = payload.schedule_cron
        if payload.timezone is not None:
            child_options["timezone"] = payload.timezone

        child = SyncConfiguration(
            name=f"{payload.name}/{repo_name}",
            provider=payload.provider,
            org_id=org_id,
            credential_id=uuid.UUID(payload.credential_id)
            if payload.credential_id
            else None,
            sync_targets=payload.sync_targets,
            sync_options=child_options,
            is_active=True,
            parent_id=parent.id,
        )
        children.append(child)

    session.add_all(children)
    await session.flush()

    return SyncConfigBatchResponse(
        parent=_sync_config_to_response(parent, children_count=len(children)),
        children=[_sync_config_to_response(c) for c in children],
        total_created=len(children),
    )


@router.post("/sync-configs", response_model=SyncConfigResponse, status_code=201)
async def create_sync_config(
    payload: SyncConfigCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)

    # Fix 1 (HIGH): Enforce repo limit before creating a new sync config.
    existing_configs = await svc.list_all(active_only=True)
    current_count = len(existing_configs)

    def _check_repo_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_repo_limit(uuid.UUID(org_id), current_count + 1)

    allowed, reason = await session.run_sync(_check_repo_limit)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason or "Repo limit exceeded")

    # Fix 5 (LOW): Validate initial_sync_depth against tier limits.
    initial_sync_depth = payload.sync_options.get("initial_sync_depth")
    if initial_sync_depth is not None:

        def _check_backfill_depth(sync_session) -> tuple[bool, str | None]:
            tier_svc = TierLimitService(sync_session)
            return tier_svc.check_backfill_limit(
                uuid.UUID(org_id), int(initial_sync_depth)
            )

        depth_allowed, depth_reason = await session.run_sync(_check_backfill_depth)
        if not depth_allowed:
            raise HTTPException(
                status_code=403,
                detail=depth_reason or "initial_sync_depth exceeds tier limit",
            )

    # Fix 3 (MEDIUM) & Fix 4 (MEDIUM): Validate schedule_cron interval and gate
    # scheduled jobs behind the "scheduled_jobs" feature (Team+ only).
    schedule_cron = payload.sync_options.get("schedule_cron")
    if schedule_cron:
        # Fix 4: Gate scheduled_jobs feature — Community tier cannot set schedules.
        async def _check_scheduled_jobs_feature(
            payload: SyncConfigCreate = payload,
            session: AsyncSession = session,
            org_id: str = org_id,
        ) -> None:
            from dev_health_ops.licensing.gating import _check_org_feature_async

            feature = "scheduled_jobs"
            if not await _check_org_feature_async(
                feature, {"session": session, "org_id": org_id}
            ):
                from dev_health_ops.licensing import has_feature

                if not has_feature(feature, log_denial=False):
                    raise HTTPException(
                        status_code=403,
                        detail="scheduled_jobs feature requires Team tier or higher",
                    )

        await _check_scheduled_jobs_feature()

        # Fix 3: Validate the cron interval against the tier's min_sync_interval_hours.
        try:
            itr = Croniter(schedule_cron)
            next1 = itr.get_next(float)
            next2 = itr.get_next(float)
            interval_hours = (next2 - next1) / 3600.0
        except Exception as exc:
            raise HTTPException(
                status_code=422, detail=f"Invalid cron expression: {exc}"
            )

        def _get_min_interval(sync_session) -> float | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "min_sync_interval_hours")
            return float(val) if val is not None else None

        min_interval = await session.run_sync(_get_min_interval)
        if min_interval is not None and interval_hours < min_interval:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Sync interval {interval_hours:.2f}h is below the minimum "
                    f"{min_interval}h allowed for your tier"
                ),
            )

    config = await svc.create(
        name=payload.name,
        provider=payload.provider,
        sync_targets=payload.sync_targets,
        sync_options=payload.sync_options,
        credential_id=payload.credential_id,
    )
    return _sync_config_to_response(config)


def _sync_config_to_response(
    c, children_count: int | None = None
) -> SyncConfigResponse:
    return SyncConfigResponse(
        id=str(c.id),
        name=c.name,
        provider=c.provider,
        credential_id=str(c.credential_id) if c.credential_id else None,
        sync_targets=c.sync_targets,
        sync_options=c.sync_options,
        is_active=c.is_active,
        parent_id=str(c.parent_id) if c.parent_id else None,
        children_count=children_count,
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

    # Fix 3 (MEDIUM) & Fix 4 (MEDIUM): Validate schedule_cron when updating sync_options.
    schedule_cron = (payload.sync_options or {}).get("schedule_cron")
    if schedule_cron:
        # Fix 4: Gate scheduled_jobs feature — Community tier cannot set schedules.
        from dev_health_ops.licensing.gating import _check_org_feature_async

        feature = "scheduled_jobs"
        if not await _check_org_feature_async(
            feature, {"session": session, "org_id": org_id}
        ):
            from dev_health_ops.licensing import has_feature

            if not has_feature(feature, log_denial=False):
                raise HTTPException(
                    status_code=403,
                    detail="scheduled_jobs feature requires Team tier or higher",
                )

        # Fix 3: Validate the cron interval against the tier's min_sync_interval_hours.
        try:
            itr = Croniter(schedule_cron)
            next1 = itr.get_next(float)
            next2 = itr.get_next(float)
            interval_hours = (next2 - next1) / 3600.0
        except Exception as exc:
            raise HTTPException(
                status_code=422, detail=f"Invalid cron expression: {exc}"
            )

        def _get_min_interval(sync_session) -> float | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "min_sync_interval_hours")
            return float(val) if val is not None else None

        min_interval = await session.run_sync(_get_min_interval)
        if min_interval is not None and interval_hours < min_interval:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Sync interval {interval_hours:.2f}h is below the minimum "
                    f"{min_interval}h allowed for your tier"
                ),
            )

    updated = await svc.update(
        name=config.name,
        sync_targets=payload.sync_targets,
        sync_options=payload.sync_options,
        is_active=payload.is_active,
    )

    # Cascade shared settings to children when updating a parent config
    if updated.parent_id is None:
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.parent_id == updated.id
        )
        result = await session.execute(stmt)
        children = result.scalars().all()
        for child in children:
            if payload.sync_targets is not None:
                child.sync_targets = payload.sync_targets
            if payload.is_active is not None:
                child.is_active = payload.is_active
            # Propagate schedule/timezone/depth from sync_options if provided
            if payload.sync_options:
                for key in ("schedule_cron", "timezone", "initial_sync_depth"):
                    if key in payload.sync_options:
                        child.sync_options = {
                            **child.sync_options,
                            key: payload.sync_options[key],
                        }
        if children:
            await session.flush()

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

    # Fix 6 (LOW-MEDIUM): Check work items count against tier limit before triggering.
    if "work-items" in (config.sync_targets or []):

        def _get_max_work_items(sync_session) -> int | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "max_work_items")
            return int(val) if val is not None else None

        max_work_items = await session.run_sync(_get_max_work_items)
        if max_work_items is not None:
            try:
                import os

                from dev_health_ops.api.queries.client import (
                    get_global_client,
                    query_dicts,
                )

                ch_uri = os.getenv("CLICKHOUSE_URI", "")
                if ch_uri:
                    client = await get_global_client(ch_uri)
                    rows = await query_dicts(
                        client,
                        "SELECT count() AS cnt FROM work_items WHERE org_id = %(org_id)s",
                        {"org_id": org_id},
                    )
                    current_count = int((rows[0].get("cnt") or 0) if rows else 0)
                    if current_count >= max_work_items:
                        raise HTTPException(
                            status_code=403,
                            detail=(
                                f"Work items limit exceeded: {current_count}/{max_work_items}. "
                                "Upgrade your tier to sync more work items."
                            ),
                        )
            except HTTPException:
                raise
            except Exception:
                # ClickHouse unavailable — allow the sync to proceed rather than block it.
                pass

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
