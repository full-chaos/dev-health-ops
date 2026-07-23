from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.org_guard import organization_exists_sync
from dev_health_ops.workers.task_utils import _as_str, cron_next_run

logger = logging.getLogger(__name__)


def _uuid_value(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _datetime_value(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    raise TypeError(f"Expected datetime, got {type(value)!r}")


@celery_app.task(
    bind=True, name="dev_health_ops.workers.tasks.dispatch_scheduled_reports"
)
def dispatch_scheduled_reports(self) -> dict:

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.reports import SavedReport
    from dev_health_ops.models.settings import JobStatus, ScheduledJob
    from dev_health_ops.reports.execution_trigger import (
        create_scheduled_report_execution,
    )

    now = datetime.now(timezone.utc)
    dispatched: list[str] = []
    committed_dispatches: list[tuple[str, str]] = []
    skipped = 0

    try:
        with get_postgres_session_sync() as session:
            with session.begin():
                jobs = (
                    session.query(ScheduledJob)
                    .filter(
                        ScheduledJob.job_type == "report",
                        ScheduledJob.status == JobStatus.ACTIVE.value,
                        ScheduledJob.is_running.is_(False),
                    )
                    .with_for_update(skip_locked=True)
                    .all()
                )

                for job in jobs:
                    if not organization_exists_sync(session, job.org_id):
                        skipped += 1
                        continue

                    report = (
                        session.query(SavedReport)
                        .filter(
                            SavedReport.schedule_id == job.id,
                            SavedReport.is_active.is_(True),
                        )
                        .one_or_none()
                    )

                    if report is None:
                        skipped += 1
                        continue

                    report_id = _uuid_value(report.id)
                    last_run = (
                        report.last_run_at
                        if isinstance(report.last_run_at, datetime)
                        else _datetime_value(report.created_at)
                    )
                    next_run = cron_next_run(
                        str(job.schedule_cron), last_run, _as_str(job.timezone)
                    )

                    if next_run <= now:
                        trigger = create_scheduled_report_execution(
                            session,
                            report,
                            job,
                            job.org_id,
                            scheduled_for=next_run,
                        )
                        job.next_run_at = cron_next_run(
                            str(job.schedule_cron), next_run, _as_str(job.timezone)
                        )
                        if trigger.dispatch_required:
                            dispatched.append(str(report_id))
                            committed_dispatches.append(
                                (trigger.report_id, trigger.run_id)
                            )
                    else:
                        skipped += 1

        from dev_health_ops.workers.report_task import execute_saved_report

        for dispatched_report_id, run_id in committed_dispatches:
            execute_saved_report.apply_async(
                kwargs={"report_id": dispatched_report_id, "run_id": run_id},
                queue="reports",
            )

    except Exception:
        logger.exception("dispatch_scheduled_reports failed")

    logger.info(
        "Scheduled report dispatch: dispatched=%d skipped=%d",
        len(dispatched),
        skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped}
