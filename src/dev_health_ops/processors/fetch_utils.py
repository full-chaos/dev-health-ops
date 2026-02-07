"""Shared fetch/batch utilities for GitHub and GitLab processors.

Extracted from duplicated patterns in github.py and gitlab.py to reduce
code duplication and provide a single source of truth for common operations.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Callable, Coroutine, List, Optional

from dev_health_ops.utils import BATCH_SIZE

logger = logging.getLogger(__name__)


def safe_parse_datetime(value: Any) -> Optional[datetime]:
    """Parse a datetime from a string or datetime, handling "Z" suffix."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


coerce_datetime = safe_parse_datetime


def extract_retry_after(exc: BaseException, connector: Any = None) -> Optional[float]:
    """Extract retry delay seconds from connector helper or exception headers."""
    retry_after: Optional[float] = None

    if connector is not None and hasattr(connector, "_rate_limit_reset_delay_seconds"):
        try:
            raw_delay = connector._rate_limit_reset_delay_seconds()
            if raw_delay is not None:
                retry_after = float(raw_delay)
                return retry_after
        except Exception:
            logger.debug("Connector retry-after extraction failed", exc_info=True)

    headers = getattr(exc, "headers", None)
    if not isinstance(headers, dict):
        return None

    headers_ci = {str(k).lower(): v for k, v in headers.items()}
    raw_retry_after = headers_ci.get("retry-after")
    if raw_retry_after is not None:
        try:
            return float(raw_retry_after)
        except (TypeError, ValueError):
            return None

    raw_reset = headers_ci.get("x-ratelimit-reset")
    if raw_reset is not None:
        try:
            return max(0.0, float(raw_reset) - time.time())
        except (TypeError, ValueError):
            return None

    return None


class SyncBatchCollector:
    """Collects items and flushes to an async store when batch_size is reached.

    For use in sync contexts (run_in_executor) that store to an async store.
    """

    def __init__(
        self,
        flush_coro_fn: Callable[[List[Any]], Coroutine[Any, Any, Any]],
        loop: asyncio.AbstractEventLoop,
        batch_size: int = BATCH_SIZE,
    ):
        self._flush_coro_fn = flush_coro_fn
        self._loop = loop
        self._batch_size = batch_size
        self._batch: list[Any] = []
        self._total = 0

    def add(self, item: Any) -> None:
        self._batch.append(item)
        self._total += 1
        if len(self._batch) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        if self._batch:
            asyncio.run_coroutine_threadsafe(
                self._flush_coro_fn(list(self._batch)),
                self._loop,
            ).result()
            self._batch.clear()

    @property
    def total(self) -> int:
        return self._total

    def __enter__(self) -> "SyncBatchCollector":
        return self

    def __exit__(self, *exc: object) -> bool:
        self.flush()
        return False


class AsyncBatchCollector:
    """Collects items and flushes to an async store when batch_size is reached.

    For use in async contexts.
    """

    def __init__(
        self,
        flush_coro_fn: Callable[[List[Any]], Coroutine[Any, Any, Any]],
        batch_size: int = BATCH_SIZE,
    ):
        self._flush_coro_fn = flush_coro_fn
        self._batch_size = batch_size
        self._batch: list[Any] = []
        self._total = 0

    def add(self, item: Any) -> None:
        self._batch.append(item)
        self._total += 1

    async def maybe_flush(self) -> None:
        if len(self._batch) >= self._batch_size:
            await self.flush()

    async def flush(self) -> None:
        if self._batch:
            await self._flush_coro_fn(list(self._batch))
            self._batch.clear()

    @property
    def total(self) -> int:
        return self._total

    async def __aenter__(self) -> "AsyncBatchCollector":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        await self.flush()
        return False
