"""Webhook event models for provider payloads.

These models represent the normalized webhook events that flow through
the system. Provider-specific payloads are validated and transformed
into these canonical forms for consistent processing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class WebhookProvider(str, Enum):
    """Supported webhook providers."""

    GITHUB = "github"
    GITLAB = "gitlab"
    JIRA = "jira"


class WebhookEventType(str, Enum):
    """Canonical event types across all providers."""

    # Git events
    PUSH = "push"
    PULL_REQUEST = "pull_request"
    MERGE_REQUEST = "merge_request"

    # Issue/Work item events
    ISSUE_CREATED = "issue_created"
    ISSUE_UPDATED = "issue_updated"
    ISSUE_CLOSED = "issue_closed"
    ISSUE_DELETED = "issue_deleted"

    # CI/CD events
    PIPELINE = "pipeline"
    DEPLOYMENT = "deployment"
    CHECK_RUN = "check_run"

    # Unknown/unsupported
    UNKNOWN = "unknown"


class WebhookEvent(BaseModel):
    """Base webhook event model.

    All webhook payloads are normalized into this format for
    consistent processing by Celery tasks.
    """

    id: UUID = Field(default_factory=uuid4, description="Unique event ID")
    provider: WebhookProvider = Field(..., description="Source provider")
    event_type: WebhookEventType = Field(..., description="Canonical event type")
    raw_event_type: str = Field(..., description="Original provider event type")
    received_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When event was received",
    )
    delivery_id: str | None = Field(
        None, description="Provider's delivery/request ID for idempotency"
    )
    org_id: str | None = Field(None, description="Organization scope")
    repo_id: str | None = Field(None, description="Repository identifier")
    repo_name: str | None = Field(None, description="Repository full name")
    payload: dict[str, Any] = Field(
        default_factory=dict, description="Raw provider payload"
    )

    model_config = ConfigDict(use_enum_values=True)


class WebhookResponse(BaseModel):
    """Response model for webhook endpoints."""

    status: Literal["accepted", "rejected", "error"] = Field(
        ..., description="Processing status"
    )
    event_id: UUID | None = Field(None, description="Assigned event ID")
    message: str = Field(default="", description="Status message")


# GitHub-specific models


class GitHubWebhookHeaders(BaseModel):
    x_github_event: str = Field(..., alias="X-GitHub-Event")
    x_github_delivery: str = Field(..., alias="X-GitHub-Delivery")
    x_hub_signature_256: str | None = Field(None, alias="X-Hub-Signature-256")

    model_config = ConfigDict(populate_by_name=True)


# GitLab-specific models


class GitLabWebhookHeaders(BaseModel):
    x_gitlab_event: str = Field(..., alias="X-Gitlab-Event")
    x_gitlab_token: str | None = Field(None, alias="X-Gitlab-Token")
    x_gitlab_instance: str | None = Field(None, alias="X-Gitlab-Instance")

    model_config = ConfigDict(populate_by_name=True)


# Jira-specific models


class JiraWebhookHeaders(BaseModel):
    x_atlassian_webhook_identifier: str | None = Field(
        None, alias="X-Atlassian-Webhook-Identifier"
    )

    model_config = ConfigDict(populate_by_name=True)


# Event type mappings


GITHUB_EVENT_MAP: dict[str, WebhookEventType] = {
    "push": WebhookEventType.PUSH,
    "pull_request": WebhookEventType.PULL_REQUEST,
    "issues": WebhookEventType.ISSUE_UPDATED,
    "issue_comment": WebhookEventType.ISSUE_UPDATED,
    "deployment": WebhookEventType.DEPLOYMENT,
    "deployment_status": WebhookEventType.DEPLOYMENT,
    "check_run": WebhookEventType.CHECK_RUN,
    "check_suite": WebhookEventType.CHECK_RUN,
}

GITLAB_EVENT_MAP: dict[str, WebhookEventType] = {
    "Push Hook": WebhookEventType.PUSH,
    "Tag Push Hook": WebhookEventType.PUSH,
    "Merge Request Hook": WebhookEventType.MERGE_REQUEST,
    "Issue Hook": WebhookEventType.ISSUE_UPDATED,
    "Pipeline Hook": WebhookEventType.PIPELINE,
    "Deployment Hook": WebhookEventType.DEPLOYMENT,
}

JIRA_EVENT_MAP: dict[str, WebhookEventType] = {
    "jira:issue_created": WebhookEventType.ISSUE_CREATED,
    "jira:issue_updated": WebhookEventType.ISSUE_UPDATED,
    "jira:issue_deleted": WebhookEventType.ISSUE_DELETED,
}


def map_github_event(event_type: str, action: str | None = None) -> WebhookEventType:
    """Map GitHub event type to canonical type."""
    if event_type == "issues":
        if action == "opened":
            return WebhookEventType.ISSUE_CREATED
        elif action == "closed":
            return WebhookEventType.ISSUE_CLOSED
        elif action == "deleted":
            return WebhookEventType.ISSUE_DELETED
        return WebhookEventType.ISSUE_UPDATED
    return GITHUB_EVENT_MAP.get(event_type, WebhookEventType.UNKNOWN)


def map_gitlab_event(event_type: str, action: str | None = None) -> WebhookEventType:
    """Map GitLab event type to canonical type."""
    if event_type == "Issue Hook":
        if action == "open":
            return WebhookEventType.ISSUE_CREATED
        elif action == "close":
            return WebhookEventType.ISSUE_CLOSED
        return WebhookEventType.ISSUE_UPDATED
    return GITLAB_EVENT_MAP.get(event_type, WebhookEventType.UNKNOWN)


def map_jira_event(webhook_event: str) -> WebhookEventType:
    """Map Jira webhook event to canonical type."""
    return JIRA_EVENT_MAP.get(webhook_event, WebhookEventType.UNKNOWN)
