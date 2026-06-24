from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.rate_limit_defer import (
    maybe_plan_rate_limit_deferral,
    plan_not_before_wait,
    reenqueue_after_rate_limit,
    reenqueue_rate_limit_chunk,
)
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
    *,
    _rate_limit_attempts: int = 0,
    _rate_limit_first_seen_at: str | None = None,
    _rate_limit_not_before: str | None = None,
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

    wait = plan_not_before_wait(_rate_limit_not_before)
    if wait is not None:
        reenqueue_rate_limit_chunk(self, wait)
        return {"status": "rate_limited_deferred", "reason": "not_before"}

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
        deferral = maybe_plan_rate_limit_deferral(
            exc,
            attempts=_rate_limit_attempts,
            first_seen_at=_rate_limit_first_seen_at,
        )
        if deferral is not None:
            logger.warning(
                "Work items sync rate-limited (provider=%s); deferring %.1fs "
                "(deferral %d)",
                provider,
                deferral.countdown,
                deferral.attempts,
            )
            reenqueue_after_rate_limit(self, deferral)
            return {
                "status": "rate_limited_deferred",
                "provider": provider,
                "retry_after_seconds": deferral.countdown,
            }
        logger.exception("Work items sync task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
