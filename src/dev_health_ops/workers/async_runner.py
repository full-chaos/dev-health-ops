"""Shared async execution helper for synchronous Python compatibility-bridge
task bodies.

These functions run synchronously by default (invoked directly, or via a
Celery `Task.run()`-shaped call from a Go HTTP compatibility bridge -- see
CHAOS-3093, PR2b: the `@celery_app.task` decorator that used to wrap these
bodies is retired along with it). This module provides a single, consistent
way to run coroutines from within such a synchronous body without creating
nested event loops or conflicting with any existing loop.

Usage
-----
    from dev_health_ops.workers.async_runner import run_async

    def my_task_body():
        result = run_async(my_coroutine())
        return result

Why not just asyncio.run()?
----------------------------
`asyncio.run()` is correct here (each call runs in a synchronous thread with
no pre-existing event loop).  However, scattered bare `asyncio.run()` calls
make it hard to:
- swap the execution strategy (e.g. uvloop, thread-pool runners)
- add uniform logging/tracing around async boundaries
- mock/patch in tests

This thin wrapper keeps the same semantics while centralising the call site.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

from .. import db

T = TypeVar("T")


def run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine synchronously from within a compatibility-bridge task body.

    Creates a fresh event loop for each call, matching the semantics of
    ``asyncio.run()``.  Safe to call from a synchronous worker thread that has
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

    db.reset_async_engines()
    return asyncio.run(coro)
