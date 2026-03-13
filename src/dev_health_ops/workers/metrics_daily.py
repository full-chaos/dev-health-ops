from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url, _invalidate_metrics_cache

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True, name="dev_health_ops.workers.tasks.dispatch_scheduled_metrics"
)
def dispatch_scheduled_metrics(self) -> dict:
    """Check ScheduledJob entries with job_type='metrics' and dispatch any that are due."""
    from croniter import croniter

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import (
        JobStatus,
        ScheduledJob,
    )

    now = datetime.now(timezone.utc)
    dispatched: list[str] = []
    skipped = 0

    try:
        with get_postgres_session_sync() as session:
            jobs = (
                session.query(ScheduledJob)
                .filter(
                    ScheduledJob.job_type == "metrics",
                    ScheduledJob.status == JobStatus.ACTIVE.value,
                )
                .all()
            )

            for job in jobs:
                if job.is_running:
                    skipped += 1
                    continue

                cron_expr = job.schedule_cron or "0 1 * * *"
                last_run = job.last_run_at or job.created_at
                cron = croniter(cron_expr, last_run)
                next_run = cron.get_next(datetime)

                if next_run <= now:
                    job_config = job.job_config or {}
                    run_daily_metrics.apply_async(
                        kwargs={
                            "db_url": job_config.get("db_url"),
                            "day": job_config.get("day"),
                            "backfill_days": job_config.get("backfill_days", 1),
                            "repo_id": job_config.get("repo_id"),
                            "repo_name": job_config.get("repo_name"),
                            "sink": job_config.get("sink", "auto"),
                            "provider": job_config.get("provider", "auto"),
                            "org_id": job.org_id or job_config.get("org_id"),
                        },
                        queue="metrics",
                    )
                    dispatched.append(str(job.id))
                else:
                    skipped += 1

    except Exception:
        logger.exception("dispatch_scheduled_metrics failed")

    logger.info(
        "Scheduled metrics dispatch: dispatched=%d skipped=%d",
        len(dispatched),
        skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped}


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="metrics",
    name="dev_health_ops.workers.tasks.run_daily_metrics",
)
def run_daily_metrics(
    self,
    db_url: str | None = None,
    day: str | None = None,
    backfill_days: int = 1,
    repo_id: str | None = None,
    repo_name: str | None = None,
    sink: str = "auto",
    provider: str = "auto",
    org_id: str | None = None,
) -> dict:
    """
    Compute and persist daily metrics asynchronously.

    Args:
        db_url: Database connection string (defaults to DATABASE_URI env)
        day: Target day as ISO string (defaults to today)
        backfill_days: Number of days to backfill
        repo_id: Optional repository UUID to filter
        repo_name: Optional repository name to filter
        sink: Sink type (auto|clickhouse|mongo|sqlite|postgres|both)
        provider: Work item provider (auto|all|jira|github|gitlab|none)
        org_id: Organization scope

    Returns:
        dict with job status and summary
    """
    from dev_health_ops.metrics.job_daily import run_daily_metrics_job

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else utc_today()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting daily metrics task: day=%s backfill=%d repo=%s",
        target_day.isoformat(),
        backfill_days,
        repo_name or str(parsed_repo_id) or "all",
    )

    try:
        # Run the async job in a new event loop
        run_async(
            run_daily_metrics_job(
                db_url=db_url,
                day=target_day,
                backfill_days=backfill_days,
                repo_id=parsed_repo_id,
                repo_name=repo_name,
                sink=sink,
                provider=provider,
                org_id=org_id or "",
            )
        )
        # Invalidate GraphQL cache after successful metrics update
        _invalidate_metrics_cache(target_day.isoformat(), "")

        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
        }
    except Exception as exc:
        logger.exception("Daily metrics task failed: %s", exc)
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
