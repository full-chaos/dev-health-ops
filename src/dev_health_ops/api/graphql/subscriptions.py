"""GraphQL subscription resolvers for real-time updates."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Optional

import strawberry
from strawberry.types import Info

from .pubsub import get_pubsub, metrics_channel, task_channel, sync_channel

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@strawberry.type
class MetricsUpdate:
    """Real-time metrics update notification."""

    org_id: str
    day: str
    updated_at: datetime
    message: str


@strawberry.type
class TaskStatus:
    """Real-time task status update."""

    task_id: str
    status: str
    progress: float
    message: Optional[str] = None
    result: Optional[str] = None
    updated_at: datetime = strawberry.field(default_factory=_utc_now)


@strawberry.type
class SyncProgress:
    """Real-time data sync progress."""

    org_id: str
    provider: str
    status: str
    items_processed: int
    items_total: int
    message: Optional[str] = None
    updated_at: datetime = strawberry.field(default_factory=_utc_now)


@strawberry.type
class Subscription:
    """Root subscription type for real-time updates."""

    @strawberry.subscription(
        description="Subscribe to metrics updates for an organization"
    )
    async def metrics_updated(
        self,
        info: Info,
        org_id: str,
    ) -> AsyncGenerator[MetricsUpdate, None]:
        """
        Subscribe to metrics update notifications.

        Yields notifications when daily metrics are computed/updated.

        Args:
            org_id: Organization ID to subscribe to.

        Yields:
            MetricsUpdate objects with update details.
        """
        pubsub = await get_pubsub()
        channel = metrics_channel(org_id)

        logger.info("Client subscribed to metrics updates for org: %s", org_id)

        async for message in pubsub.subscribe(channel):
            try:
                yield MetricsUpdate(
                    org_id=org_id,
                    day=message.data.get("day", ""),
                    updated_at=datetime.fromisoformat(
                        message.data.get("updated_at", _utc_now().isoformat())
                    ),
                    message=message.data.get("message", "Metrics updated"),
                )
            except Exception as e:
                logger.warning("Error processing metrics update: %s", e)

    @strawberry.subscription(description="Subscribe to task status updates")
    async def task_status(
        self,
        info: Info,
        task_id: str,
    ) -> AsyncGenerator[TaskStatus, None]:
        """
        Subscribe to task status updates.

        Yields notifications as a background task progresses.

        Args:
            task_id: Celery task ID to monitor.

        Yields:
            TaskStatus objects with progress updates.
        """
        pubsub = await get_pubsub()
        channel = task_channel(task_id)

        logger.info("Client subscribed to task status: %s", task_id)

        async for message in pubsub.subscribe(channel):
            try:
                yield TaskStatus(
                    task_id=task_id,
                    status=message.data.get("status", "unknown"),
                    progress=float(message.data.get("progress", 0)),
                    message=message.data.get("message"),
                    result=message.data.get("result"),
                    updated_at=datetime.fromisoformat(
                        message.data.get("updated_at", _utc_now().isoformat())
                    ),
                )
            except Exception as e:
                logger.warning("Error processing task status: %s", e)

    @strawberry.subscription(description="Subscribe to data sync progress")
    async def sync_progress(
        self,
        info: Info,
        org_id: str,
    ) -> AsyncGenerator[SyncProgress, None]:
        """
        Subscribe to data sync progress updates.

        Yields notifications as data is synced from external providers.

        Args:
            org_id: Organization ID to subscribe to.

        Yields:
            SyncProgress objects with sync status.
        """
        pubsub = await get_pubsub()
        channel = sync_channel(org_id)

        logger.info("Client subscribed to sync progress for org: %s", org_id)

        async for message in pubsub.subscribe(channel):
            try:
                yield SyncProgress(
                    org_id=org_id,
                    provider=message.data.get("provider", "unknown"),
                    status=message.data.get("status", "unknown"),
                    items_processed=int(message.data.get("items_processed", 0)),
                    items_total=int(message.data.get("items_total", 0)),
                    message=message.data.get("message"),
                    updated_at=datetime.fromisoformat(
                        message.data.get("updated_at", _utc_now().isoformat())
                    ),
                )
            except Exception as e:
                logger.warning("Error processing sync progress: %s", e)


# Helper functions for publishing updates from tasks


async def publish_metrics_update(
    org_id: str,
    day: str,
    message: str = "Daily metrics updated",
) -> None:
    """
    Publish a metrics update notification.

    Call this from background tasks after computing metrics.

    Args:
        org_id: Organization ID.
        day: The day that was updated (YYYY-MM-DD).
        message: Optional message.
    """
    pubsub = await get_pubsub()
    await pubsub.publish(
        metrics_channel(org_id),
        {
            "day": day,
            "message": message,
            "updated_at": _utc_now().isoformat(),
        },
    )


async def publish_task_status(
    task_id: str,
    status: str,
    progress: float = 0.0,
    message: Optional[str] = None,
    result: Optional[str] = None,
) -> None:
    """
    Publish a task status update.

    Call this from background tasks to report progress.

    Args:
        task_id: Celery task ID.
        status: Current status (pending, running, completed, failed).
        progress: Progress percentage (0-100).
        message: Optional status message.
        result: Optional result summary (for completed tasks).
    """
    pubsub = await get_pubsub()
    await pubsub.publish(
        task_channel(task_id),
        {
            "status": status,
            "progress": progress,
            "message": message,
            "result": result,
            "updated_at": _utc_now().isoformat(),
        },
    )


async def publish_sync_progress(
    org_id: str,
    provider: str,
    status: str,
    items_processed: int = 0,
    items_total: int = 0,
    message: Optional[str] = None,
) -> None:
    """
    Publish a sync progress update.

    Call this from sync tasks to report progress.

    Args:
        org_id: Organization ID.
        provider: Provider being synced (github, gitlab, jira).
        status: Current status (starting, syncing, completed, failed).
        items_processed: Number of items processed so far.
        items_total: Total items to process.
        message: Optional status message.
    """
    pubsub = await get_pubsub()
    await pubsub.publish(
        sync_channel(org_id),
        {
            "provider": provider,
            "status": status,
            "items_processed": items_processed,
            "items_total": items_total,
            "message": message,
            "updated_at": _utc_now().isoformat(),
        },
    )
