from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url, _invalidate_sync_cache

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="sync",
    name="dev_health_ops.workers.tasks.run_work_items_sync",
)
def run_work_items_sync(
    self,
    db_url: str | None = None,
    provider: str = "auto",
    since_days: int = 30,
    org_id: str = "",
) -> dict:
    """
    Sync work items from external providers.

    Args:
        db_url: Database connection string
        provider: Provider to sync from (auto|jira|github|gitlab|all)
        since_days: Number of days to look back

    Returns:
        dict with sync status and counts
    """

    db_url = db_url or _get_db_url()
    since = datetime.now(timezone.utc) - timedelta(days=since_days)

    logger.info(
        "Starting work items sync task: provider=%s since=%s",
        provider,
        since.isoformat(),
    )

    try:
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

        # run_work_items_sync_job is synchronous
        run_work_items_sync_job(
            db_url=db_url,
            day=since.date(),
            backfill_days=since_days,
            provider=provider,
            org_id=org_id,
        )

        # Invalidate GraphQL cache after successful sync
        _invalidate_sync_cache(provider, "")

        return {
            "status": "success",
            "provider": provider,
            "since_days": since_days,
        }
    except Exception as exc:
        logger.exception("Work items sync task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
