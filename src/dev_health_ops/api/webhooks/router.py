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
import os
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

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
from dev_health_ops.db import get_postgres_session

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

        getattr(process_webhook_event, "delay")(
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
        repo_name=None,
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


@router.post("/license", response_model=WebhookResponse)
async def license_webhook(
    request: Request,
    session: AsyncSession = Depends(get_postgres_session),
) -> WebhookResponse:
    """Handle license/entitlement change notifications from license-svc.

    Called by license-svc after Stripe webhook events that change an org's tier.
    Updates Organization.tier in the database.
    """
    webhook_secret = os.getenv("LICENSE_WEBHOOK_SECRET")
    if webhook_secret:
        provided_secret = request.headers.get("x-webhook-secret")
        if not provided_secret or provided_secret != webhook_secret:
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    from .models import LicenseWebhookPayload

    try:
        payload = LicenseWebhookPayload(**body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")

    from dev_health_ops.models.licensing import OrgLicense, Tier

    tier_map = {
        "community": Tier.FREE,
        "free": Tier.FREE,
        "team": Tier.STARTER,
        "starter": Tier.STARTER,
        "pro": Tier.PRO,
        "enterprise": Tier.ENTERPRISE,
    }
    new_tier = tier_map.get(payload.tier.lower(), Tier.FREE)

    from dev_health_ops.models.users import Organization
    from sqlalchemy import select

    org = None
    try:
        org_uuid = uuid.UUID(payload.org_id)
    except (ValueError, TypeError):
        org_uuid = None

    if org_uuid:
        result = await session.execute(
            select(Organization).where(Organization.id == org_uuid)
        )
        org = result.scalar_one_or_none()

    if org:
        org.tier = str(new_tier.value)
        await session.flush()

        from datetime import datetime, timezone

        result = await session.execute(
            select(OrgLicense).where(OrgLicense.org_id == org_uuid)
        )
        org_license = result.scalar_one_or_none()

        if org_license is None:
            org_license = OrgLicense(
                org_id=org_uuid,
                tier=str(new_tier.value),
                license_type="saas",
            )
            session.add(org_license)
        else:
            org_license.tier = str(new_tier.value)

        if payload.licensed_users is not None:
            org_license.licensed_users = payload.licensed_users
        if payload.licensed_repos is not None:
            org_license.licensed_repos = payload.licensed_repos
        if payload.customer_id is not None:
            org_license.customer_id = payload.customer_id
        if payload.features_override is not None:
            org_license.features_override = payload.features_override
        if payload.limits_override is not None:
            org_license.limits_override = payload.limits_override
        if payload.expires_at is not None:
            from dev_health_ops.processors.fetch_utils import safe_parse_datetime

            parsed_expires = safe_parse_datetime(payload.expires_at)
            if parsed_expires:
                org_license.expires_at = parsed_expires

        org_license.is_valid = True
        org_license.last_validated_at = datetime.now(timezone.utc)

        await session.flush()
        logger.info(
            "Updated org tier: org_id=%s tier=%s action=%s",
            payload.org_id,
            payload.tier,
            payload.action,
        )
    else:
        logger.warning(
            "License webhook for unknown org: org_id=%s",
            payload.org_id,
        )

    event_id = uuid.uuid4()
    return WebhookResponse(
        status="accepted",
        event_id=event_id,
        message=f"License event '{payload.action}' processed for org {payload.org_id}",
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
