"""Base processor providing shared orchestration patterns.

Extracted from common patterns in github.py and gitlab.py processors.
Existing processor functions remain as-is; new processors can subclass
BaseProcessor for standardized pipeline management.
"""

from __future__ import annotations

import abc
import asyncio
import logging
from collections.abc import Callable
from typing import Any, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")

_sentinel = object()


class BaseProcessor(abc.ABC, Generic[T, R]):
    """Abstract base for processors that process items through a concurrent pipeline.

    Encapsulates the common pattern:
    1. Create a results queue with bounded size
    2. Spawn a consumer task that drains results
    3. Process items concurrently with a semaphore
    4. Wait for all results to be consumed
    """

    def __init__(
        self,
        *,
        max_concurrent: int = 4,
        logger: logging.Logger | None = None,
    ) -> None:
        self.max_concurrent = max(1, max_concurrent)
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @abc.abstractmethod
    async def process_single(self, item: T) -> R:
        """Process a single item. Subclasses must implement."""

    @abc.abstractmethod
    async def store_result(self, result: R) -> None:
        """Persist a single result. Subclasses must implement."""

    async def run_sync_in_executor(self, func: Callable[..., Any], *args: Any) -> Any:
        """Run a synchronous function in the default executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    async def process_batch(self, items: list[T]) -> int:
        """Process a batch of items through the concurrent pipeline."""
        if not items:
            return 0

        results_queue: asyncio.Queue[Any] = asyncio.Queue(
            maxsize=max(1, self.max_concurrent * 2)
        )
        processed = 0
        errors = 0

        async def _consume() -> None:
            while True:
                result = await results_queue.get()
                try:
                    if result is _sentinel:
                        return
                    await self.store_result(result)
                finally:
                    results_queue.task_done()

        consumer = asyncio.create_task(_consume())
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def _process_one(item: T) -> None:
            nonlocal processed, errors
            async with semaphore:
                try:
                    result = await self.process_single(item)
                    await results_queue.put(result)
                    processed += 1
                except Exception as exc:
                    errors += 1
                    self.logger.warning("Failed to process item: %s", exc)

        tasks = [asyncio.create_task(_process_one(item)) for item in items]
        await asyncio.gather(*tasks, return_exceptions=True)

        await results_queue.join()
        await results_queue.put(_sentinel)
        await consumer

        if errors:
            self.logger.info(
                "Batch complete: %d processed, %d errors", processed, errors
            )

        return processed

    async def on_before_batch(self, items: list[T]) -> list[T]:
        """Hook called before batch processing."""
        return items

    async def on_after_batch(self, processed_count: int) -> None:
        """Hook called after batch processing completes."""

    async def run(self, items: list[T]) -> int:
        """Full pipeline: before_hook -> process_batch -> after_hook."""
        filtered_items = await self.on_before_batch(items)
        count = await self.process_batch(filtered_items)
        await self.on_after_batch(count)
        return count
