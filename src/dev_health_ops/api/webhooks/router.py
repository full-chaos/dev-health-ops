"""Webhook router with provider-specific endpoints.

All webhooks follow the same pattern:
1. Validate signature/token (via dependency)
2. Parse provider-specific headers
3. Create canonical WebhookEvent
4. Dispatch to Celery task for async processing
5. Return accepted response immediately

This ensures webhooks don't timeout during heavy processing.
"""

from __future__ import annotations

import json
import logging
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Request

from .auth import GitHubWebhookBody, GitLabWebhookBody, JiraWebhookBody
from .models import (
    WebhookEvent,
    WebhookEventType,
    WebhookProvider,
    WebhookResponse,
    map_github_event,
    map_gitlab_event,
    map_jira_event,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/webhooks", tags=["webhooks"])


def _dispatch_webhook_task(event: WebhookEvent) -> None:
    """Dispatch webhook event to Celery for async processing.

    This is a best-effort dispatch - if Celery is unavailable,
    we log and continue (the event is lost, but the webhook
    doesn't fail catastrophically).
    """
    try:
        from dev_health_ops.workers.tasks import process_webhook_event

        process_webhook_event.delay(
            provider=event.provider,
            event_type=event.event_type,
            delivery_id=event.delivery_id,
            payload=event.payload,
            org_id=event.org_id,
            repo_name=event.repo_name,
        )
        logger.info(
            "Dispatched webhook event: provider=%s type=%s delivery=%s",
            event.provider,
            event.event_type,
            event.delivery_id,
        )
    except Exception as e:
        # Log but don't fail - webhook should still return 200
        # to prevent provider retries flooding the system
        logger.error(
            "Failed to dispatch webhook to Celery: %s (event_id=%s)",
            e,
            event.id,
        )


@router.post("/github", response_model=WebhookResponse)
async def github_webhook(
    request: Request,
    body: GitHubWebhookBody,
    x_github_event: Annotated[str, Header()],
    x_github_delivery: Annotated[str, Header()],
) -> WebhookResponse:
    """Handle GitHub webhook events.

    Supports: push, pull_request, issues, deployment, check_run, check_suite

    The signature is validated before this handler is called via
    the GitHubWebhookBody dependency.
    """
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON in GitHub webhook: %s", e)
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Extract action for more specific event mapping
    action = payload.get("action")
    event_type = map_github_event(x_github_event, action)

    # Extract repository info
    repo = payload.get("repository", {})
    repo_name = repo.get("full_name")
    org = payload.get("organization", {})
    org_id = org.get("login") if org else repo.get("owner", {}).get("login")

    event = WebhookEvent(
        provider=WebhookProvider.GITHUB,
        event_type=event_type,
        raw_event_type=f"{x_github_event}.{action}" if action else x_github_event,
        delivery_id=x_github_delivery,
        org_id=org_id,
        repo_id=None,
        repo_name=repo_name,
        payload=payload,
    )

    if event_type == WebhookEventType.UNKNOWN:
        sanitized_event = x_github_event.replace("\r", "").replace("\n", "")
        logger.debug("Ignoring unsupported GitHub event: %s", sanitized_event)
        return WebhookResponse(
            status="accepted",
            event_id=event.id,
            message=f"Event type '{x_github_event}' not processed",
        )

    _dispatch_webhook_task(event)

    return WebhookResponse(
        status="accepted",
        event_id=event.id,
        message=f"Processing {event_type.value} event",
    )


@router.post("/gitlab", response_model=WebhookResponse)
async def gitlab_webhook(
    request: Request,
    body: GitLabWebhookBody,
    x_gitlab_event: Annotated[str, Header()],
) -> WebhookResponse:
    """Handle GitLab webhook events.

    Supports: Push Hook, Merge Request Hook, Issue Hook, Pipeline Hook

    The token is validated before this handler is called via
    the GitLabWebhookBody dependency.
    """
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON in GitLab webhook: %s", e)
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Extract object attributes for action
    object_attrs = payload.get("object_attributes", {})
    action = object_attrs.get("action") or object_attrs.get("state")
    event_type = map_gitlab_event(x_gitlab_event, action)

    # Extract project/repo info
    project = payload.get("project", {})
    repo_name = project.get("path_with_namespace")

    # GitLab uses numeric IDs, construct org from namespace
    namespace = project.get("namespace")
    org_id = namespace if isinstance(namespace, str) else None

    # Generate a delivery ID from object kind and ID
    object_id = object_attrs.get("id") or payload.get("object_kind")
    delivery_id = f"{x_gitlab_event}:{object_id}" if object_id else x_gitlab_event

    event = WebhookEvent(
        provider=WebhookProvider.GITLAB,
        event_type=event_type,
        raw_event_type=f"{x_gitlab_event}:{action}" if action else x_gitlab_event,
        delivery_id=delivery_id,
        org_id=org_id,
        repo_id=None,
        repo_name=repo_name,
        payload=payload,
    )

    if event_type == WebhookEventType.UNKNOWN:
        safe_gitlab_event = (
            x_gitlab_event.replace("\r", "").replace("\n", "")
            if x_gitlab_event is not None
            else ""
        )
        logger.debug("Ignoring unsupported GitLab event: %s", safe_gitlab_event)
        return WebhookResponse(
            status="accepted",
            event_id=event.id,
            message=f"Event type '{safe_gitlab_event}' not processed",
        )

    _dispatch_webhook_task(event)

    return WebhookResponse(
        status="accepted",
        event_id=event.id,
        message=f"Processing {event_type.value} event",
    )


@router.post("/jira", response_model=WebhookResponse)
async def jira_webhook(
    request: Request,
    body: JiraWebhookBody,
) -> WebhookResponse:
    """Handle Jira webhook events.

    Supports: jira:issue_created, jira:issue_updated, jira:issue_deleted

    Jira webhooks include the event type in the payload body,
    not in headers like GitHub/GitLab.
    """
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON in Jira webhook: %s", e)
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Jira event type is in the payload
    webhook_event = payload.get("webhookEvent", "")
    event_type = map_jira_event(webhook_event)

    # Extract issue info
    issue = payload.get("issue", {})
    issue_key = issue.get("key")
    fields = issue.get("fields", {})
    project = fields.get("project", {})
    project_key = project.get("key")

    # Use project key as org_id for Jira
    org_id = project_key

    # Generate delivery ID from timestamp and issue
    timestamp = payload.get("timestamp", "")
    delivery_id = f"{webhook_event}:{issue_key}:{timestamp}" if issue_key else None

    event = WebhookEvent(
        provider=WebhookProvider.JIRA,
        event_type=event_type,
        raw_event_type=webhook_event,
        delivery_id=delivery_id,
        org_id=org_id,
        repo_id=project_key,  # Jira projects map to repo concept
        payload=payload,
    )

    if event_type == WebhookEventType.UNKNOWN:
        safe_webhook_event = str(webhook_event).replace("\r", "").replace("\n", "")
        logger.debug("Ignoring unsupported Jira event: %s", safe_webhook_event)
        return WebhookResponse(
            status="accepted",
            event_id=event.id,
            message=f"Event type '{safe_webhook_event}' not processed",
        )

    _dispatch_webhook_task(event)

    return WebhookResponse(
        status="accepted",
        event_id=event.id,
        message=f"Processing {event_type.value} event",
    )


@router.get("/health")
async def webhooks_health() -> dict:
    """Health check for webhook endpoints.

    Verifies:
    - Router is mounted
    - Celery connection (if configured)
    - Webhook secrets are configured
    """
    import os

    secrets_configured = {
        "github": bool(os.getenv("GITHUB_WEBHOOK_SECRET")),
        "gitlab": bool(os.getenv("GITLAB_WEBHOOK_TOKEN")),
        "jira": bool(os.getenv("JIRA_WEBHOOK_SECRET")),
    }

    celery_available = False
    try:
        from dev_health_ops.workers.celery_app import celery_app
        celery_available = celery_app is not None

    except Exception as exc:
        # If Celery is not configured or unavailable, log and report as not available.
        logger.warning("Celery health check failed in /webhooks/health: %s", exc)
        pass

    return {
        "status": "ok",
        "secrets_configured": secrets_configured,
        "celery_available": celery_available,
    }
