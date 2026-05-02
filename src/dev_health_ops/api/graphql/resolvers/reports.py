from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import strawberry
from sqlalchemy import func, select
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


_SAVED_REPORT_COLUMNS = (
    SavedReport.id,
    SavedReport.org_id,
    SavedReport.name,
    SavedReport.description,
    SavedReport.report_plan,
    SavedReport.is_template,
    SavedReport.template_source_id,
    SavedReport.parameters,
    SavedReport.schedule_id,
    SavedReport.is_active,
    SavedReport.last_run_at,
    SavedReport.last_run_status,
    SavedReport.created_at,
    SavedReport.updated_at,
    SavedReport.created_by,
)

_REPORT_RUN_COLUMNS = (
    ReportRun.id,
    ReportRun.report_id,
    ReportRun.status,
    ReportRun.started_at,
    ReportRun.completed_at,
    ReportRun.duration_seconds,
    ReportRun.rendered_markdown,
    ReportRun.artifact_url,
    ReportRun.provenance_records,
    ReportRun.error,
    ReportRun.triggered_by,
    ReportRun.created_at,
)


def _uuid_value(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _uuid_value_or_none(value: object | None) -> uuid.UUID | None:
    if value is None:
        return None
    return _uuid_value(value)


def _datetime_value(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    raise TypeError(f"Expected datetime, got {type(value)!r}")


def _datetime_or_none(value: object | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    return None


def _float_or_none(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value))


def _string_value(value: object | None) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


def _string_or_none(value: object | None) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def _bool_value(value: object | None) -> bool:
    return value if isinstance(value, bool) else bool(value)


def _json_object(value: object | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): raw_value for key, raw_value in value.items()}


def _json_output(value: object | None) -> Any:
    return value


def _to_saved_report_type(
    *,
    report_id: uuid.UUID,
    org_id: str,
    name: str,
    description: str | None,
    report_plan: Any,
    is_template: bool,
    template_source_id: uuid.UUID | None,
    parameters: Any,
    schedule_id: uuid.UUID | None,
    is_active: bool,
    last_run_at: datetime | None,
    last_run_status: str | None,
    created_at: datetime,
    updated_at: datetime,
    created_by: str | None,
) -> SavedReportType:
    return SavedReportType(
        id=str(report_id),
        org_id=org_id,
        name=name,
        description=description,
        report_plan=report_plan,
        is_template=is_template,
        template_source_id=str(template_source_id) if template_source_id else None,
        parameters=parameters,
        schedule_id=str(schedule_id) if schedule_id else None,
        is_active=is_active,
        last_run_at=last_run_at,
        last_run_status=last_run_status,
        created_at=created_at,
        updated_at=updated_at,
        created_by=created_by,
    )


def _saved_report_type_from_row(row: Any) -> SavedReportType:
    return _to_saved_report_type(
        report_id=_uuid_value(row.id),
        org_id=_string_value(row.org_id),
        name=_string_value(row.name),
        description=_string_or_none(row.description),
        report_plan=_json_output(row.report_plan),
        is_template=_bool_value(row.is_template),
        template_source_id=_uuid_value_or_none(row.template_source_id),
        parameters=_json_output(row.parameters),
        schedule_id=_uuid_value_or_none(row.schedule_id),
        is_active=_bool_value(row.is_active),
        last_run_at=_datetime_or_none(row.last_run_at),
        last_run_status=_string_or_none(row.last_run_status),
        created_at=_datetime_value(row.created_at),
        updated_at=_datetime_value(row.updated_at),
        created_by=_string_or_none(row.created_by),
    )


def _to_report_run_type(
    *,
    run_id: uuid.UUID,
    report_id: uuid.UUID,
    status: str,
    started_at: datetime | None,
    completed_at: datetime | None,
    duration_seconds: float | None,
    rendered_markdown: str | None,
    artifact_url: str | None,
    provenance_records: Any,
    error: str | None,
    triggered_by: str,
    created_at: datetime,
) -> ReportRunType:
    return ReportRunType(
        id=str(run_id),
        report_id=str(report_id),
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration_seconds,
        rendered_markdown=rendered_markdown,
        artifact_url=artifact_url,
        provenance_records=provenance_records,
        error=error,
        triggered_by=triggered_by,
        created_at=created_at,
    )


def _report_run_type_from_row(row: Any) -> ReportRunType:
    return _to_report_run_type(
        run_id=_uuid_value(row.id),
        report_id=_uuid_value(row.report_id),
        status=_string_value(row.status),
        started_at=_datetime_or_none(row.started_at),
        completed_at=_datetime_or_none(row.completed_at),
        duration_seconds=_float_or_none(row.duration_seconds),
        rendered_markdown=_string_or_none(row.rendered_markdown),
        artifact_url=_string_or_none(row.artifact_url),
        provenance_records=_json_output(row.provenance_records),
        error=_string_or_none(row.error),
        triggered_by=_string_value(row.triggered_by),
        created_at=_datetime_value(row.created_at),
    )


async def _load_saved_report_type(
    session: AsyncSession,
    report_id: uuid.UUID,
    org_id: str | None = None,
) -> SavedReportType | None:
    stmt = select(*_SAVED_REPORT_COLUMNS).where(SavedReport.id == report_id)
    if org_id is not None:
        stmt = stmt.where(SavedReport.org_id == org_id)
    result = await session.execute(stmt)
    row = result.one_or_none()
    if row is None:
        return None
    return _saved_report_type_from_row(row)


async def _load_report_run_rows(
    session: AsyncSession,
    report_id: uuid.UUID,
    limit: int,
) -> list[ReportRunType]:
    result = await session.execute(
        select(*_REPORT_RUN_COLUMNS)
        .where(ReportRun.report_id == report_id)
        .order_by(ReportRun.created_at.desc())
        .limit(limit)
    )
    return [_report_run_type_from_row(row) for row in result.all()]


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
            setattr(existing_job, "schedule_cron", cron)
            setattr(existing_job, "timezone", tz)
            setattr(existing_job, "updated_at", datetime.now(timezone.utc))
            return

    job = ScheduledJob(
        name=f"report:{report.name}",
        job_type="report",
        schedule_cron=cron,
        org_id=_string_value(report.org_id),
        job_config={"report_id": str(_uuid_value(report.id))},
        tz=tz,
        status=JobStatus.ACTIVE.value,
    )
    session.add(job)
    await session.flush()
    setattr(report, "schedule_id", _uuid_value(job.id))


async def resolve_saved_reports(
    org_id: str,
    limit: int = 50,
    offset: int = 0,
) -> SavedReportConnection:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        count_result = await session.execute(
            select(func.count())
            .select_from(SavedReport)
            .where(SavedReport.org_id == org_id)
        )
        total = count_result.scalar() or 0

        result = await session.execute(
            select(*_SAVED_REPORT_COLUMNS)
            .where(SavedReport.org_id == org_id)
            .order_by(SavedReport.updated_at.desc())
            .offset(offset)
            .limit(limit)
        )
        reports = result.all()

    return SavedReportConnection(
        items=[_saved_report_type_from_row(row) for row in reports],
        total=total,
    )


async def resolve_saved_report(
    org_id: str,
    report_id: str,
) -> SavedReportType | None:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        return await _load_saved_report_type(session, uuid.UUID(report_id), org_id)


async def resolve_report_runs(
    org_id: str,
    report_id: str,
    limit: int = 50,
) -> ReportRunConnection:
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        report_uuid = uuid.UUID(report_id)
        report_result = await session.execute(
            select(SavedReport.id).where(
                SavedReport.org_id == org_id,
                SavedReport.id == report_uuid,
            )
        )
        report_row = report_result.one_or_none()
        if report_row is None:
            return ReportRunConnection(items=[], total=0)

        resolved_report_id = _uuid_value(report_row.id)

        count_result = await session.execute(
            select(func.count())
            .select_from(ReportRun)
            .where(ReportRun.report_id == resolved_report_id)
        )
        total = count_result.scalar() or 0
        runs = await _load_report_run_rows(session, resolved_report_id, limit)

    return ReportRunConnection(
        items=runs,
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
            report_plan=_json_object(input.report_plan),
            is_template=input.is_template,
            parameters=_json_object(input.parameters),
        )
        session.add(report)
        await session.flush()
        report_uuid = _uuid_value(report.id)

        if input.schedule_cron:
            await _ensure_or_update_schedule(
                session, report, input.schedule_cron, input.schedule_timezone
            )

        report_type = await _load_saved_report_type(session, report_uuid, org_id)

    if report_type is None:
        raise ValueError(f"Saved report not found after create: {report_uuid}")
    return report_type


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
            setattr(report, "name", input.name)
        if input.description is not None:
            setattr(report, "description", input.description)
        if input.report_plan is not None:
            setattr(report, "report_plan", _json_object(input.report_plan))
        if input.is_template is not None:
            setattr(report, "is_template", input.is_template)
        if input.parameters is not None:
            setattr(report, "parameters", _json_object(input.parameters))
        if input.is_active is not None:
            setattr(report, "is_active", input.is_active)

        setattr(report, "updated_at", datetime.now(timezone.utc))
        report_uuid = _uuid_value(report.id)

        if input.schedule_cron is not None:
            await _ensure_or_update_schedule(
                session,
                report,
                input.schedule_cron,
                input.schedule_timezone or "UTC",
            )

        return await _load_saved_report_type(session, report_uuid, org_id)


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
            parameter_overrides=_json_object(input.parameter_overrides),
        )
        session.add(cloned)
        await session.flush()

        return await _load_saved_report_type(session, _uuid_value(cloned.id), org_id)


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

        report_uuid = _uuid_value(report.id)

        run = ReportRun(
            report_id=report_uuid,
            triggered_by="api",
            status=ReportRunStatus.PENDING.value,
        )
        session.add(run)
        await session.flush()
        run_id_uuid = _uuid_value(run.id)

    try:
        from dev_health_ops.workers.report_task import execute_saved_report

        execute_saved_report.apply_async(
            kwargs={"report_id": str(report_uuid), "run_id": str(run_id_uuid)},
            queue="reports",
        )
    except (ImportError, AttributeError):
        # Celery may not be available in test/dev environments;
        # the report run record is still created for manual pickup.
        pass

    return _to_report_run_type(
        run_id=run_id_uuid,
        report_id=report_uuid,
        status=ReportRunStatus.PENDING.value,
        started_at=None,
        completed_at=None,
        duration_seconds=None,
        rendered_markdown=None,
        artifact_url=None,
        provenance_records=None,
        error=None,
        triggered_by="api",
        created_at=_datetime_value(run.created_at),
    )
