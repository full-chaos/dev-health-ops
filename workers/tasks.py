"""Celery task definitions for background job processing.

These tasks wrap the existing metrics jobs to enable async execution:
- run_daily_metrics: Compute and persist daily metrics
- run_complexity_job: Analyze code complexity
- run_work_items_sync: Sync work items from providers
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import date
from typing import Optional

from workers.celery_app import celery_app

logger = logging.getLogger(__name__)


def _get_db_url() -> str:
    """Get database URL from environment."""
    return os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL") or ""


@celery_app.task(bind=True, max_retries=3, queue="metrics")
def run_daily_metrics(
    self,
    db_url: Optional[str] = None,
    day: Optional[str] = None,
    backfill_days: int = 1,
    repo_id: Optional[str] = None,
    repo_name: Optional[str] = None,
    sink: str = "auto",
    provider: str = "auto",
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

    Returns:
        dict with job status and summary
    """
    from metrics.job_daily import run_daily_metrics_job

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else date.today()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting daily metrics task: day=%s backfill=%d repo=%s",
        target_day.isoformat(),
        backfill_days,
        repo_name or str(parsed_repo_id) or "all",
    )

    try:
        # Run the async job in a new event loop
        asyncio.run(
            run_daily_metrics_job(
                db_url=db_url,
                day=target_day,
                backfill_days=backfill_days,
                repo_id=parsed_repo_id,
                repo_name=repo_name,
                sink=sink,
                provider=provider,
            )
        )
        # Invalidate GraphQL cache after successful metrics update
        _invalidate_metrics_cache(target_day.isoformat())

        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
        }
    except Exception as exc:
        logger.exception("Daily metrics task failed: %s", exc)
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


def _invalidate_metrics_cache(day: str, org_id: str = "default") -> None:
    """Invalidate GraphQL caches after metrics update."""
    try:
        from api.graphql.cache_invalidation import invalidate_on_metrics_update
        from api.services.cache import create_cache

        cache = create_cache(ttl_seconds=300)
        count = invalidate_on_metrics_update(cache, org_id, day)
        logger.info("Invalidated %d cache entries after metrics update", count)
    except Exception as e:
        logger.warning("Cache invalidation failed (non-fatal): %s", e)


def _invalidate_sync_cache(sync_type: str, org_id: str = "default") -> None:
    """Invalidate GraphQL caches after data sync."""
    try:
        from api.graphql.cache_invalidation import invalidate_on_sync_complete
        from api.services.cache import create_cache

        cache = create_cache(ttl_seconds=300)
        count = invalidate_on_sync_complete(cache, org_id, sync_type)
        logger.info("Invalidated %d cache entries after %s sync", count, sync_type)
    except Exception as e:
        logger.warning("Cache invalidation failed (non-fatal): %s", e)


@celery_app.task(bind=True, max_retries=3, queue="metrics")
def run_complexity_job(
    self,
    db_url: Optional[str] = None,
    repo_id: Optional[str] = None,
    repo_name: Optional[str] = None,
) -> dict:
    """
    Analyze code complexity for repositories.

    Note: This task requires a repo_path which needs to be discovered.
    For full complexity analysis, use the CLI directly.

    Args:
        db_url: Database connection string
        repo_id: Optional repository UUID to filter
        repo_name: Optional repository name to filter

    Returns:
        dict with job status
    """
    db_url = db_url or _get_db_url()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting complexity analysis task: repo=%s",
        repo_name or str(parsed_repo_id) or "all",
    )

    # Return skipped status since this requires repo_path parameter
    # In production, this would need to be enhanced to discover repo paths
    return {
        "status": "skipped",
        "reason": "complexity task requires repo_path - use CLI instead",
        "repo_id": repo_id or "all",
    }


@celery_app.task(bind=True, max_retries=3, queue="sync")
def run_work_items_sync(
    self,
    db_url: Optional[str] = None,
    provider: str = "auto",
    since_days: int = 30,
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
    from datetime import datetime, timedelta, timezone

    db_url = db_url or _get_db_url()
    since = datetime.now(timezone.utc) - timedelta(days=since_days)

    logger.info(
        "Starting work items sync task: provider=%s since=%s",
        provider,
        since.isoformat(),
    )

    try:
        from metrics.job_work_items import run_work_items_sync_job

        # run_work_items_sync_job is synchronous
        run_work_items_sync_job(
            db_url=db_url,
            day=since.date(),
            backfill_days=since_days,
            provider=provider,
        )

        # Invalidate GraphQL cache after successful sync
        _invalidate_sync_cache(provider)

        return {
            "status": "success",
            "provider": provider,
            "since_days": since_days,
        }
    except Exception as exc:
        logger.exception("Work items sync task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(bind=True)
def health_check(self) -> dict:
    """Simple health check task to verify worker is running."""
    return {
        "status": "healthy",
        "worker_id": self.request.id,
    }
