from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.integrations import SyncRun, SyncRunStatus
from dev_health_ops.models.settings import JobRun, JobRunStatus, ScheduledJob


async def fetch_latest_successful_sync_at(
    session: AsyncSession,
    *,
    org_id: str,
) -> datetime | None:
    successful_runs = union_all(
        select(SyncRun.completed_at.label("completed_at")).where(
            SyncRun.org_id == org_id,
            SyncRun.status == SyncRunStatus.SUCCESS.value,
            SyncRun.completed_at.is_not(None),
        ),
        select(JobRun.completed_at.label("completed_at"))
        .join(ScheduledJob, ScheduledJob.id == JobRun.job_id)
        .where(
            ScheduledJob.org_id == org_id,
            ScheduledJob.job_type == "sync",
            JobRun.status == JobRunStatus.SUCCESS.value,
            JobRun.completed_at.is_not(None),
        ),
    ).subquery()
    latest = (
        await session.execute(select(func.max(successful_runs.c.completed_at)))
    ).scalar_one_or_none()
    if latest is None:
        return None
    if latest.tzinfo is None:
        return latest.replace(tzinfo=timezone.utc)
    return latest.astimezone(timezone.utc)
