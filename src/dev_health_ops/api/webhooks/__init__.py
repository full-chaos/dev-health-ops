"""Webhook ingestion endpoints for real-time sync.

This module provides webhook endpoints for:
- GitHub webhooks (push, PR, issues, deployments)
- GitLab webhooks (push, MR, issues, pipelines)
- Jira webhooks (issue created/updated/transitioned)

Each provider has secure signature validation to ensure payloads
are authentic. Events are processed asynchronously via Celery tasks.
"""

from .router import router

__all__ = ["router"]
