"""Shared async execution helper for Celery tasks.

Celery tasks are synchronous by default. This module provides a single,
consistent way to run coroutines from within a Celery task body without
creating nested event loops or conflicting with any existing loop.

Usage
-----
    from dev_health_ops.workers.async_runner import run_async

    @celery_app.task(bind=True)
    def my_task(self):
        result = run_async(my_coroutine())
        return result

Why not just asyncio.run()?
----------------------------
`asyncio.run()` is correct inside Celery tasks (each task runs in a
synchronous worker thread with no pre-existing event loop).  However,
scattered bare `asyncio.run()` calls make it hard to:
- swap the execution strategy (e.g. uvloop, thread-pool runners)
- add uniform logging/tracing around async boundaries
- mock/patch in tests

This thin wrapper keeps the same semantics while centralising the call site.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


def run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine synchronously from within a Celery task.

    Creates a fresh event loop for each call, matching the semantics of
    ``asyncio.run()``.  Safe to call from Celery worker threads that have
    no pre-existing event loop.

    Args:
        coro: An awaitable coroutine to execute.

    Returns:
        The return value of the coroutine.

    Raises:
        RuntimeError: If called from within an already-running event loop
            (e.g. inside an async context — use ``await`` directly instead).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        raise RuntimeError(
            "run_async() called from within a running event loop. "
            "Use 'await' directly instead of run_async() inside async functions."
        )

    return asyncio.run(coro)
