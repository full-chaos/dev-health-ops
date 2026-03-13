from __future__ import annotations

import logging
from datetime import datetime, timezone

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.sync_batch import _is_batch_eligible, dispatch_batch_sync
from dev_health_ops.workers.sync_runtime import run_sync_config

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True, name="dev_health_ops.workers.tasks.dispatch_scheduled_syncs"
)
def dispatch_scheduled_syncs(self) -> dict:
    """Check active sync configs and dispatch any that are due."""
    from croniter import croniter

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import (
        ScheduledJob,
        SyncConfiguration,
    )

    now = datetime.now(timezone.utc)
    dispatched: list[str] = []
    skipped = 0

    try:
        with get_postgres_session_sync() as session:
            configs = (
                session.query(SyncConfiguration)
                .filter(SyncConfiguration.is_active.is_(True))
                .all()
            )

            for config in configs:
                job = (
                    session.query(ScheduledJob)
                    .filter(
                        ScheduledJob.sync_config_id == config.id,
                        ScheduledJob.org_id == config.org_id,
                        ScheduledJob.job_type == "sync",
                    )
                    .one_or_none()
                )

                if job and job.is_running:
                    skipped += 1
                    continue

                cron_expr = job.schedule_cron if job else "0 * * * *"
                last_sync = config.last_sync_at or config.created_at
                cron = croniter(cron_expr, last_sync)
                next_run = cron.get_next(datetime)

                if next_run <= now:
                    if _is_batch_eligible(config):
                        dispatch_batch_sync.apply_async(
                            kwargs={
                                "config_id": str(config.id),
                                "org_id": config.org_id,
                                "triggered_by": "schedule",
                            },
                            queue="sync",
                        )
                    else:
                        run_sync_config.apply_async(
                            kwargs={
                                "config_id": str(config.id),
                                "org_id": config.org_id,
                                "triggered_by": "schedule",
                            },
                            queue="sync",
                        )
                    dispatched.append(str(config.id))
                else:
                    skipped += 1

    except Exception:
        logger.exception("dispatch_scheduled_syncs failed")

    logger.info(
        "Scheduled sync dispatch: dispatched=%d skipped=%d",
        len(dispatched),
        skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped}
