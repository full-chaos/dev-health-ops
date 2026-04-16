"""Rate-limit helper shared across provider clients.

Wraps the common ``wait_sync() / reset() / penalize()`` boilerplate around
API calls into a single context manager. Use as::

    with gate_call(self.gate):
        result = self.api.do_something()

On normal exit the gate is reset (clearing backoff state). On exception
the gate is penalized (deferring the next allowed call). The exception
propagates by default; pass ``swallow=True`` when the caller prefers
to log-and-continue.

For explicit server-provided delays (e.g. HTTP 429 ``Retry-After``
header), use the helper form::

    with gate_call(self.gate, retry_after=retry_after_seconds):
        ...

``retry_after`` is passed through to ``gate.penalize`` only on failure.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from dev_health_ops.connectors.utils.rate_limit_queue import RateLimitGate


@contextmanager
def gate_call(
    gate: RateLimitGate,
    *,
    retry_after: float | None = None,
    swallow: bool = False,
) -> Iterator[None]:
    """Context manager that wraps a gated API call.

    Args:
        gate: the ``RateLimitGate`` to use.
        retry_after: optional explicit delay (seconds) to pass to
            ``gate.penalize`` if the wrapped block raises.
        swallow: if True, exceptions are logged via ``penalize`` and
            suppressed (the ``with`` block exits normally).
    """
    gate.wait_sync()
    try:
        yield
    except Exception:
        gate.penalize(retry_after)
        if swallow:
            return
        raise
    else:
        gate.reset()


def penalize_from_response(
    gate: RateLimitGate, response: Any, *, default: float | None = None
) -> float:
    """Apply a penalty driven by an HTTP response's ``Retry-After`` header.

    Returns the applied delay (seconds). ``response`` is any object with a
    ``headers`` mapping. Invalid / missing headers fall back to ``default``.
    """
    retry_after = default
    try:
        raw = response.headers.get("Retry-After") if response is not None else None
        if raw is not None:
            retry_after = float(raw)
    except (TypeError, ValueError):
        retry_after = default
    return gate.penalize(retry_after)
