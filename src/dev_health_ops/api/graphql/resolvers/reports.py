from __future__ import annotations

import uuid
from datetime import datetime, timezone

import strawberry
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.reports import ReportRun, ReportRunStatus, SavedReport
from dev_health_ops.models.settings import JobStatus, ScheduledJob


@strawberry.type
class SavedReportType:
    id: str
    org_id: str
    name: str
    description: str | None
    report_plan: strawberry.scalars.JSON
    is_template: bool
    template_source_id: str | None
    parameters: strawberry.scalars.JSON | None
    schedule_id: str | None
    is_active: bool
    last_run_at: datetime | None
    last_run_status: str | None
    created_at: datetime
    updated_at: datetime
    created_by: str | None


@strawberry.type
class ReportRunType:
    id: str
    report_id: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    duration_seconds: float | None
    rendered_markdown: str | None
    artifact_url: str | None
    provenance_records: strawberry.scalars.JSON | None
    error: str | None
    triggered_by: str
    created_at: datetime


@strawberry.type
class SavedReportConnection:
    items: list[SavedReportType]
    total: int


@strawberry.type
class ReportRunConnection:
    items: list[ReportRunType]
    total: int


@strawberry.input
class CreateSavedReportInput:
    name: str
    description: str | None = None
    report_plan: strawberry.scalars.JSON | None = None
    is_template: bool = False
    parameters: strawberry.scalars.JSON | None = None
    schedule_cron: str | None = None
    schedule_timezone: str = "UTC"


@strawberry.input
class UpdateSavedReportInput:
    name: str | None = None
    description: str | None = None
    report_plan: strawberry.scalars.JSON | None = None
    is_template: bool | None = None
    parameters: strawberry.scalars.JSON | None = None
    is_active: bool | None = None
    schedule_cron: str | None = None
    schedule_timezone: str | None = None


@strawberry.input
class CloneSavedReportInput:
    source_report_id: str
    new_name: str | None = None
    parameter_overrides: strawberry.scalars.JSON | None = None


def _to_saved_report_type(report: SavedReport) -> SavedReportType:
    return SavedReportType(
        id=str(report.id),
        org_id=report.org_id,
        name=report.name,
        description=report.description,
        report_plan=report.report_plan,
        is_template=report.is_template,
        template_source_id=str(report.template_source_id)
        if report.template_source_id
        else None,
        parameters=report.parameters,
        schedule_id=str(report.schedule_id) if report.schedule_id else None,
        is_active=report.is_active,
        last_run_at=report.last_run_at,
        last_run_status=report.last_run_status,
        created_at=report.created_at,
        updated_at=report.updated_at,
        created_by=report.created_by,
    )


def _to_report_run_type(run: ReportRun) -> ReportRunType:
    return ReportRunType(
        id=str(run.id),
        report_id=str(run.report_id),
        status=run.status,
        started_at=run.started_at,
        completed_at=run.completed_at,
        duration_seconds=run.duration_seconds,
        rendered_markdown=run.rendered_markdown,
        artifact_url=run.artifact_url,
        provenance_records=run.provenance_records,
        error=run.error,
        triggered_by=run.triggered_by,
        created_at=run.created_at,
    )


async def _get_session(context) -> AsyncSession:
    from dev_health_ops.db import get_postgres_session

    return get_postgres_session()


async def _ensure_or_update_schedule(
    session: AsyncSession,
    report: SavedReport,
    cron: str | None,
    tz: str = "UTC",
) -> None:
    if cron is None:
        return

    if report.schedule_id:
        result = await session.execute(
            select(ScheduledJob).where(ScheduledJob.id == report.schedule_id)
        )
        existing_job = result.scalar_one_or_none()
        if existing_job:
            existing_job.schedule_cron = cron
            existing_job.timezone = tz
            existing_job.updated_at = datetime.now(timezone.utc)
            return

    job = ScheduledJob(
        name=f"report:{report.name}",
        job_type="report",
        schedule_cron=cron,
        org_id=report.org_id,
        job_config={"report_id": str(report.id)},
        tz=tz,
        status=JobStatus.ACTIVE.value,
    )
    session.add(job)
    await session.flush()
    report.schedule_id = job.id


async def resolve_saved_reports(
    org_id: str,
    limit: int = 50,
    offset: int = 0,
) -> SavedReportConnection:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        count_result = await session.execute(
            select(SavedReport).where(SavedReport.org_id == org_id)
        )
        total = len(count_result.scalars().all())

        result = await session.execute(
            select(SavedReport)
            .where(SavedReport.org_id == org_id)
            .order_by(SavedReport.updated_at.desc())
            .offset(offset)
            .limit(limit)
        )
        reports = result.scalars().all()

    return SavedReportConnection(
        items=[_to_saved_report_type(r) for r in reports],
        total=total,
    )


async def resolve_saved_report(
    org_id: str,
    report_id: str,
) -> SavedReportType | None:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        result = await session.execute(
            select(SavedReport).where(
                SavedReport.org_id == org_id,
                SavedReport.id == uuid.UUID(report_id),
            )
        )
        report = result.scalar_one_or_none()

    if report is None:
        return None
    return _to_saved_report_type(report)


async def resolve_report_runs(
    org_id: str,
    report_id: str,
    limit: int = 50,
) -> ReportRunConnection:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        report_result = await session.execute(
            select(SavedReport).where(
                SavedReport.org_id == org_id,
                SavedReport.id == uuid.UUID(report_id),
            )
        )
        report = report_result.scalar_one_or_none()
        if report is None:
            return ReportRunConnection(items=[], total=0)

        count_result = await session.execute(
            select(ReportRun).where(ReportRun.report_id == report.id)
        )
        total = len(count_result.scalars().all())

        result = await session.execute(
            select(ReportRun)
            .where(ReportRun.report_id == report.id)
            .order_by(ReportRun.created_at.desc())
            .limit(limit)
        )
        runs = result.scalars().all()

    return ReportRunConnection(
        items=[_to_report_run_type(r) for r in runs],
        total=total,
    )


async def resolve_create_saved_report(
    org_id: str,
    input: CreateSavedReportInput,
) -> SavedReportType:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        report = SavedReport(
            name=input.name,
            org_id=org_id,
            description=input.description,
            report_plan=input.report_plan or {},
            is_template=input.is_template,
            parameters=input.parameters or {},
        )
        session.add(report)
        await session.flush()

        if input.schedule_cron:
            await _ensure_or_update_schedule(
                session, report, input.schedule_cron, input.schedule_timezone
            )

    return _to_saved_report_type(report)


async def resolve_update_saved_report(
    org_id: str,
    report_id: str,
    input: UpdateSavedReportInput,
) -> SavedReportType | None:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        result = await session.execute(
            select(SavedReport).where(
                SavedReport.org_id == org_id,
                SavedReport.id == uuid.UUID(report_id),
            )
        )
        report = result.scalar_one_or_none()
        if report is None:
            return None

        if input.name is not None:
            report.name = input.name
        if input.description is not None:
            report.description = input.description
        if input.report_plan is not None:
            report.report_plan = input.report_plan
        if input.is_template is not None:
            report.is_template = input.is_template
        if input.parameters is not None:
            report.parameters = input.parameters
        if input.is_active is not None:
            report.is_active = input.is_active

        report.updated_at = datetime.now(timezone.utc)

        if input.schedule_cron is not None:
            await _ensure_or_update_schedule(
                session,
                report,
                input.schedule_cron,
                input.schedule_timezone or "UTC",
            )

    return _to_saved_report_type(report)


async def resolve_delete_saved_report(
    org_id: str,
    report_id: str,
) -> bool:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        result = await session.execute(
            select(SavedReport).where(
                SavedReport.org_id == org_id,
                SavedReport.id == uuid.UUID(report_id),
            )
        )
        report = result.scalar_one_or_none()
        if report is None:
            return False

        await session.delete(report)

    return True


async def resolve_clone_saved_report(
    org_id: str,
    input: CloneSavedReportInput,
) -> SavedReportType | None:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        result = await session.execute(
            select(SavedReport).where(
                SavedReport.org_id == org_id,
                SavedReport.id == uuid.UUID(input.source_report_id),
            )
        )
        source = result.scalar_one_or_none()
        if source is None:
            return None

        cloned = source.clone(
            new_name=input.new_name,
            parameter_overrides=input.parameter_overrides,
        )
        session.add(cloned)

    return _to_saved_report_type(cloned)


async def resolve_trigger_report(
    org_id: str,
    report_id: str,
) -> ReportRunType | None:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        result = await session.execute(
            select(SavedReport).where(
                SavedReport.org_id == org_id,
                SavedReport.id == uuid.UUID(report_id),
            )
        )
        report = result.scalar_one_or_none()
        if report is None:
            return None

        run = ReportRun(
            report_id=report.id,
            triggered_by="api",
            status=ReportRunStatus.PENDING.value,
        )
        session.add(run)
        await session.flush()

    try:
        from dev_health_ops.workers.report_task import execute_saved_report

        execute_saved_report.apply_async(
            kwargs={"report_id": str(report.id), "run_id": str(run.id)},
            queue="reports",
        )
    except (ImportError, AttributeError):
        pass

    return _to_report_run_type(run)
