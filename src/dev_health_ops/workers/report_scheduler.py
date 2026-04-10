from __future__ import annotations

import logging
from datetime import datetime, timezone

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True, name="dev_health_ops.workers.tasks.dispatch_scheduled_reports"
)
def dispatch_scheduled_reports(self) -> dict:
    from croniter import croniter

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.reports import ReportRun, ReportRunStatus, SavedReport
    from dev_health_ops.models.settings import JobStatus, ScheduledJob

    now = datetime.now(timezone.utc)
    dispatched: list[str] = []
    skipped = 0

    try:
        with get_postgres_session_sync() as session:
            jobs = (
                session.query(ScheduledJob)
                .filter(
                    ScheduledJob.job_type == "report",
                    ScheduledJob.status == JobStatus.ACTIVE.value,
                    ScheduledJob.is_running.is_(False),
                )
                .all()
            )

            for job in jobs:
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

                last_run = report.last_run_at or report.created_at
                cron = croniter(job.schedule_cron, last_run)
                next_run = cron.get_next(datetime)

                if next_run <= now:
                    run = ReportRun(
                        report_id=report.id,
                        triggered_by="scheduler",
                        status=ReportRunStatus.PENDING.value,
                    )
                    session.add(run)
                    session.flush()

                    from dev_health_ops.workers.report_task import (
                        execute_saved_report,
                    )

                    execute_saved_report.apply_async(
                        kwargs={
                            "report_id": str(report.id),
                            "run_id": str(run.id),
                        },
                        queue="reports",
                    )
                    dispatched.append(str(report.id))
                else:
                    skipped += 1

    except Exception:
        logger.exception("dispatch_scheduled_reports failed")

    logger.info(
        "Scheduled report dispatch: dispatched=%d skipped=%d",
        len(dispatched),
        skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped}
