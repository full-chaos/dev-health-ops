"""Redis PubSub implementation for GraphQL subscriptions."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class PubSubMessage:
    """A message from the PubSub system."""

    channel: str
    data: Dict[str, Any]


class RedisPubSub:
    """
    Redis-based PubSub for GraphQL subscriptions.

    Provides async publish/subscribe functionality using Redis pub/sub.
    Falls back to in-memory channels if Redis is unavailable.
    """

    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize the PubSub system.

        Args:
            redis_url: Redis connection URL. Defaults to REDIS_URL env var.
        """
        self._redis_url = redis_url or os.getenv("REDIS_URL")
        self._client: Optional[Any] = None
        self._pubsub: Optional[Any] = None
        self._available = False
        self._memory_channels: Dict[str, asyncio.Queue] = {}

    async def connect(self) -> bool:
        """
        Connect to Redis.

        Returns:
            True if connected, False if using memory fallback.
        """
        if not self._redis_url:
            logger.info("No Redis URL configured, using memory pubsub")
            return False

        try:
            import redis.asyncio as aioredis

            self._client = aioredis.from_url(self._redis_url, decode_responses=True)
            await self._client.ping()
            self._available = True
            logger.info("Connected to Redis pubsub")
            return True
        except Exception as e:
            logger.warning("Redis unavailable, using memory pubsub: %s", e)
            self._available = False
            return False

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._pubsub:
            await self._pubsub.close()
            self._pubsub = None
        if self._client:
            await self._client.close()
            self._client = None
        self._available = False

    async def publish(self, channel: str, data: Dict[str, Any]) -> int:
        """
        Publish a message to a channel.

        Args:
            channel: Channel name.
            data: Message data (will be JSON serialized).

        Returns:
            Number of subscribers that received the message.
        """
        message = json.dumps(data)

        if self._available and self._client:
            try:
                return await self._client.publish(channel, message)
            except Exception as e:
                logger.warning("Redis publish failed: %s", e)

        # Memory fallback
        if channel in self._memory_channels:
            queue = self._memory_channels[channel]
            await queue.put(PubSubMessage(channel=channel, data=data))
            return 1
        return 0

    async def subscribe(self, channel: str) -> AsyncIterator[PubSubMessage]:
        """
        Subscribe to a channel and yield messages.

        Args:
            channel: Channel name to subscribe to.

        Yields:
            PubSubMessage objects as they arrive.
        """
        if self._available and self._client:
            async for msg in self._subscribe_redis(channel):
                yield msg
        else:
            async for msg in self._subscribe_memory(channel):
                yield msg

    async def _subscribe_redis(self, channel: str) -> AsyncIterator[PubSubMessage]:
        """Subscribe using Redis pub/sub."""
        try:
            pubsub = self._client.pubsub()
            await pubsub.subscribe(channel)
            logger.debug("Subscribed to Redis channel: %s", channel)

            while True:
                try:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                    if message and message["type"] == "message":
                        data = json.loads(message["data"])
                        yield PubSubMessage(channel=channel, data=data)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warning("Error receiving message: %s", e)
                    await asyncio.sleep(0.1)
        finally:
            if pubsub:
                await pubsub.unsubscribe(channel)
                await pubsub.close()

    async def _subscribe_memory(self, channel: str) -> AsyncIterator[PubSubMessage]:
        """Subscribe using in-memory queue."""
        if channel not in self._memory_channels:
            self._memory_channels[channel] = asyncio.Queue()

        queue = self._memory_channels[channel]
        logger.debug("Subscribed to memory channel: %s", channel)

        try:
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield message
                except asyncio.TimeoutError:
                    # Heartbeat - keeps the connection alive
                    continue
                except asyncio.CancelledError:
                    break
        finally:
            # Cleanup empty channels
            if (
                channel in self._memory_channels
                and self._memory_channels[channel].empty()
            ):
                del self._memory_channels[channel]


# Global PubSub instance
_pubsub: Optional[RedisPubSub] = None


async def get_pubsub() -> RedisPubSub:
    """Get or create the global PubSub instance."""
    global _pubsub
    if _pubsub is None:
        _pubsub = RedisPubSub()
        await _pubsub.connect()
    return _pubsub


# Channel name constants
CHANNEL_METRICS_UPDATED = "graphql:metrics:{org_id}"
CHANNEL_TASK_STATUS = "graphql:task:{task_id}"
CHANNEL_SYNC_PROGRESS = "graphql:sync:{org_id}"


def metrics_channel(org_id: str) -> str:
    """Get the metrics update channel for an org."""
    return CHANNEL_METRICS_UPDATED.format(org_id=org_id)


def task_channel(task_id: str) -> str:
    """Get the task status channel for a task."""
    return CHANNEL_TASK_STATUS.format(task_id=task_id)


def sync_channel(org_id: str) -> str:
    """Get the sync progress channel for an org."""
    return CHANNEL_SYNC_PROGRESS.format(org_id=org_id)
