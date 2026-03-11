from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.backfill import BackfillJob


class BackfillJobService:
    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def create_job(
        self,
        sync_config_id: str,
        since: date,
        before: date,
        total_chunks: int,
        celery_task_id: str | None = None,
    ) -> BackfillJob:
        job = BackfillJob(
            org_id=self.org_id,
            sync_config_id=uuid.UUID(sync_config_id),
            celery_task_id=celery_task_id,
            status="pending",
            since_date=since,
            before_date=before,
            total_chunks=total_chunks,
        )
        self.session.add(job)
        await self.session.flush()
        return job

    async def get_job(self, job_id: str) -> BackfillJob | None:
        stmt = select(BackfillJob).where(
            BackfillJob.id == uuid.UUID(job_id),
            BackfillJob.org_id == self.org_id,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_jobs(
        self, limit: int = 50, offset: int = 0
    ) -> tuple[list[BackfillJob], int]:
        count_stmt = (
            select(func.count())
            .where(BackfillJob.org_id == self.org_id)
            .select_from(BackfillJob)
        )
        total = (await self.session.execute(count_stmt)).scalar() or 0

        stmt = (
            select(BackfillJob)
            .where(BackfillJob.org_id == self.org_id)
            .order_by(BackfillJob.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all()), int(total)

    async def update_progress(
        self,
        job_id: str,
        completed_chunks: int,
        failed_chunks: int,
        status: str | None = None,
    ) -> None:
        job = await self.get_job(job_id)
        if job is None:
            return
        job_obj = cast(Any, job)
        job_obj.completed_chunks = completed_chunks
        job_obj.failed_chunks = failed_chunks
        if status:
            job_obj.status = status
        await self.session.flush()

    async def mark_running(self, job_id: str) -> None:
        job = await self.get_job(job_id)
        if job:
            job_obj = cast(Any, job)
            job_obj.status = "running"
            job_obj.started_at = datetime.now(timezone.utc)
            await self.session.flush()

    async def mark_completed(self, job_id: str) -> None:
        job = await self.get_job(job_id)
        if job:
            job_obj = cast(Any, job)
            if job_obj.started_at is None:
                job_obj.started_at = datetime.now(timezone.utc)
            job_obj.status = "completed"
            job_obj.completed_at = datetime.now(timezone.utc)
            await self.session.flush()

    async def mark_failed(self, job_id: str, error: str) -> None:
        job = await self.get_job(job_id)
        if job:
            job_obj = cast(Any, job)
            if job_obj.started_at is None:
                job_obj.started_at = datetime.now(timezone.utc)
            job_obj.status = "failed"
            job_obj.error_message = error
            job_obj.completed_at = datetime.now(timezone.utc)
            await self.session.flush()
