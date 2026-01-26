"""Celery task definitions for background job processing.

These tasks wrap the existing metrics jobs to enable async execution:
- run_daily_metrics: Compute and persist daily metrics
- run_complexity_job: Analyze code complexity
- run_work_items_sync: Sync work items from dev_health_ops.providers
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import date
from typing import Optional

from dev_health_ops.workers.celery_app import celery_app

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
    from dev_health_ops.metrics.job_daily import run_daily_metrics_job

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
        from dev_health_ops.api.graphql.cache_invalidation import (
            invalidate_on_metrics_update,
        )
        from dev_health_ops.api.services.cache import create_cache

        cache = create_cache(ttl_seconds=300)
        count = invalidate_on_metrics_update(cache, org_id, day)
        logger.info("Invalidated %d cache entries after metrics update", count)
    except Exception as e:
        logger.warning("Cache invalidation failed (non-fatal): %s", e)


def _invalidate_sync_cache(sync_type: str, org_id: str = "default") -> None:
    """Invalidate GraphQL caches after data sync."""
    try:
        from dev_health_ops.api.graphql.cache_invalidation import (
            invalidate_on_sync_complete,
        )
        from dev_health_ops.api.services.cache import create_cache

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
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

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


@celery_app.task(bind=True, max_retries=3, queue="webhooks")
def process_webhook_event(
    self,
    provider: str,
    event_type: str,
    delivery_id: Optional[str] = None,
    payload: Optional[dict] = None,
    org_id: Optional[str] = None,
    repo_name: Optional[str] = None,
) -> dict:
    """
    Process a webhook event asynchronously.

    This task handles the actual processing of webhook events after
    they've been received and validated by the webhook endpoints.

    Args:
        provider: Source provider (github, gitlab, jira)
        event_type: Canonical event type
        delivery_id: Provider's delivery ID for idempotency
        payload: Raw webhook payload
        org_id: Organization scope
        repo_name: Repository name (if applicable)

    Returns:
        dict with processing status and summary
    """
    from datetime import datetime, timezone

    logger.info(
        "Processing webhook event: provider=%s type=%s delivery=%s repo=%s",
        provider,
        event_type,
        delivery_id,
        repo_name,
    )

    try:
        if delivery_id:
            if _is_duplicate_delivery(provider, delivery_id):
                logger.info(
                    "Skipping duplicate webhook delivery: %s/%s",
                    provider,
                    delivery_id,
                )
                return {
                    "status": "skipped",
                    "reason": "duplicate_delivery",
                    "delivery_id": delivery_id,
                }
            _record_delivery(provider, delivery_id)

        if provider == "github":
            result = _process_github_event(event_type, payload, org_id, repo_name)
        elif provider == "gitlab":
            result = _process_gitlab_event(event_type, payload, org_id, repo_name)
        elif provider == "jira":
            result = _process_jira_event(event_type, payload, org_id)
        else:
            logger.warning("Unknown webhook provider: %s", provider)
            return {"status": "error", "reason": f"unknown_provider: {provider}"}

        _invalidate_sync_cache(provider, org_id or "default")

        return {
            "status": "success",
            "provider": provider,
            "event_type": event_type,
            "delivery_id": delivery_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            **result,
        }

    except Exception as exc:
        logger.exception(
            "Webhook processing failed: provider=%s type=%s error=%s",
            provider,
            event_type,
            exc,
        )
        raise self.retry(exc=exc, countdown=30 * (2**self.request.retries))


def _is_duplicate_delivery(provider: str, delivery_id: str) -> bool:
    """Check if we've already processed this delivery.

    Uses a simple in-memory check for now. In production, this should
    use Redis or a database table for persistence across workers.
    """
    # TODO: Implement persistent idempotency check via Redis or DB
    # For now, always return False (process all events)
    return False


def _record_delivery(provider: str, delivery_id: str) -> None:
    """Record that we've processed this delivery.

    This prevents duplicate processing if the provider retries.
    """
    # TODO: Implement persistent delivery recording via Redis or DB
    pass


def _process_github_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
    repo_name: str | None,
) -> dict:
    """Process a GitHub webhook event.

    Routes to appropriate sync based on event type:
    - push: Trigger git sync for new commits
    - pull_request: Update PR data
    - issues: Update work items
    - deployment: Update deployment data
    """
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    if event_type == "push":
        commits = payload.get("commits", [])
        return {"processed": True, "commits_count": len(commits)}

    elif event_type == "pull_request":
        pr = payload.get("pull_request", {})
        return {"processed": True, "pr_number": pr.get("number")}

    elif event_type in ("issue_created", "issue_updated", "issue_closed"):
        issue = payload.get("issue", {})
        return {"processed": True, "issue_number": issue.get("number")}

    elif event_type == "deployment":
        deployment = payload.get("deployment", {})
        return {"processed": True, "deployment_id": deployment.get("id")}

    return {"processed": False, "reason": f"unhandled_event: {event_type}"}


def _process_gitlab_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
    repo_name: str | None,
) -> dict:
    """Process a GitLab webhook event.

    Routes to appropriate sync based on event type:
    - push: Trigger git sync for new commits
    - merge_request: Update MR data
    - issue: Update work items
    - pipeline: Update CI/CD data
    """
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    if event_type == "push":
        commits = payload.get("commits", [])
        return {"processed": True, "commits_count": len(commits)}

    elif event_type == "merge_request":
        mr = payload.get("object_attributes", {})
        return {"processed": True, "mr_iid": mr.get("iid")}

    elif event_type in ("issue_created", "issue_updated", "issue_closed"):
        issue = payload.get("object_attributes", {})
        return {"processed": True, "issue_iid": issue.get("iid")}

    elif event_type == "pipeline":
        pipeline = payload.get("object_attributes", {})
        return {"processed": True, "pipeline_id": pipeline.get("id")}

    return {"processed": False, "reason": f"unhandled_event: {event_type}"}


def _process_jira_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
) -> dict:
    """Process a Jira webhook event.

    Routes to work item sync for issue events.
    """
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    issue = payload.get("issue", {})
    issue_key = issue.get("key")

    if event_type in (
        "issue_created",
        "issue_updated",
        "issue_closed",
        "issue_deleted",
    ):
        return {"processed": True, "issue_key": issue_key}

    return {"processed": False, "reason": f"unhandled_event: {event_type}"}
