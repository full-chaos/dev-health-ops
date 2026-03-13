from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url, _invalidate_sync_cache

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="webhooks",
    name="dev_health_ops.workers.tasks.process_webhook_event",
)
def process_webhook_event(
    self,
    provider: str,
    event_type: str,
    delivery_id: str | None = None,
    payload: dict | None = None,
    org_id: str | None = None,
    repo_name: str | None = None,
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

        _invalidate_sync_cache(provider, org_id)

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

    Uses Redis for persistence across workers if available, otherwise
    falls back to a simple in-memory check.
    """
    cache_key = f"webhook_delivery:{provider}:{delivery_id}"
    try:
        from dev_health_ops.core.cache import create_cache

        # Use a short TTL for idempotency (e.g., 24 hours)
        cache = create_cache(ttl_seconds=86400)
        return cache.get(cache_key) is not None
    except Exception as e:
        logger.warning("Idempotency check failed (falling back to False): %s", e)
        return False


def _record_delivery(provider: str, delivery_id: str) -> None:
    """Record that we've processed this delivery.

    This prevents duplicate processing if the provider retries.
    """
    cache_key = f"webhook_delivery:{provider}:{delivery_id}"
    try:
        from dev_health_ops.core.cache import create_cache

        cache = create_cache(ttl_seconds=86400)
        cache.set(cache_key, "processed")
    except Exception as e:
        logger.warning("Failed to record webhook delivery: %s", e)


def _process_github_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
    repo_name: str | None,
) -> dict:
    """Process a GitHub webhook event."""
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    # Import specialized sync processors
    from dev_health_ops.processors.github import process_github_repo
    from dev_health_ops.storage import resolve_db_type, run_with_store

    db_url = _get_db_url()
    db_type = resolve_db_type(db_url, None)

    # Repository owner and name from payload if not provided
    repo_payload = payload.get("repository", {})
    owner = repo_payload.get("owner", {}).get("login")
    repo = repo_payload.get("name")

    if not (owner and repo):
        # Fallback to provided repo_name if possible
        if repo_name and "/" in repo_name:
            owner, repo = repo_name.split("/", 1)
        else:
            return {"processed": False, "reason": "missing_repo_info"}

    token = os.getenv("GITHUB_TOKEN") or ""
    if not token:
        return {"processed": False, "reason": "missing_github_token"}

    async def _sync_handler(store):
        if event_type == "push":
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=True,
                sync_prs=False,
                sync_cicd=False,
            )
        elif event_type == "pull_request":
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=False,
                sync_prs=True,
                sync_cicd=False,
            )
        elif event_type in ("issue_created", "issue_updated", "issue_closed"):
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=False,
                sync_prs=False,
                sync_incidents=True,
            )
        elif event_type == "deployment":
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=False,
                sync_prs=False,
                sync_deployments=True,
            )
        elif event_type == "workflow_run":
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=False,
                sync_prs=False,
                sync_cicd=True,
            )

    # Execute sync
    try:
        run_async(run_with_store(db_url, db_type, _sync_handler, org_id=org_id))
        return {"processed": True, "repo": f"{owner}/{repo}", "event": event_type}
    except Exception as e:
        logger.error("Failed to process GitHub webhook %s: %s", event_type, e)
        return {"processed": False, "error": str(e)}


def _process_gitlab_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
    repo_name: str | None,
) -> dict:
    """Process a GitLab webhook event."""
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    from dev_health_ops.processors.gitlab import process_gitlab_project
    from dev_health_ops.storage import resolve_db_type, run_with_store

    db_url = _get_db_url()
    db_type = resolve_db_type(db_url, None)

    # Project ID from payload
    project_payload = payload.get("project", {})
    project_id = project_payload.get("id")

    if not project_id:
        return {"processed": False, "reason": "missing_project_id"}

    token = os.getenv("GITLAB_TOKEN") or ""
    gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com")
    if not token:
        return {"processed": False, "reason": "missing_gitlab_token"}

    async def _sync_handler(store):
        if event_type == "push":
            await process_gitlab_project(
                store=store,
                project_id=project_id,
                token=token,
                gitlab_url=gitlab_url,
                sync_git=True,
                sync_prs=False,
                sync_cicd=False,
            )
        elif event_type == "merge_request":
            await process_gitlab_project(
                store=store,
                project_id=project_id,
                token=token,
                gitlab_url=gitlab_url,
                sync_git=False,
                sync_prs=True,
                sync_cicd=False,
            )
        elif event_type in ("issue_created", "issue_updated", "issue_closed"):
            await process_gitlab_project(
                store=store,
                project_id=project_id,
                token=token,
                gitlab_url=gitlab_url,
                sync_git=False,
                sync_prs=False,
                sync_incidents=True,
            )
        elif event_type == "pipeline":
            await process_gitlab_project(
                store=store,
                project_id=project_id,
                token=token,
                gitlab_url=gitlab_url,
                sync_git=False,
                sync_prs=False,
                sync_cicd=True,
            )

    try:
        run_async(run_with_store(db_url, db_type, _sync_handler, org_id=org_id))
        return {"processed": True, "project_id": project_id, "event": event_type}
    except Exception as e:
        logger.error("Failed to process GitLab webhook %s: %s", event_type, e)
        return {"processed": False, "error": str(e)}


def _process_jira_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
) -> dict:
    """Process a Jira webhook event."""
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

    try:
        # Jira sync doesn't have a single-issue sync yet, so we trigger a broad sync
        # for the provider. In a production system, we'd optimize this to sync only
        # the specific issue key.
        run_work_items_sync_job(
            db_url=_get_db_url(),
            day=utc_today(),
            backfill_days=1,
            provider="jira",
            org_id=org_id or "",
        )
        return {"processed": True, "event": event_type}
    except Exception as e:
        logger.error("Failed to process Jira webhook %s: %s", event_type, e)
        return {"processed": False, "error": str(e)}
