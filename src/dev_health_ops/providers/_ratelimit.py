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
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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
    except Exception as exc:
        effective_retry_after = retry_after
        if effective_retry_after is None:
            candidate = getattr(exc, "retry_after_seconds", None)
            if isinstance(candidate, int | float):
                effective_retry_after = float(candidate)
        gate.penalize(effective_retry_after)
        if swallow:
            return
        raise
    else:
        gate.reset()


def parse_retry_after_header(headers: Any) -> float | None:
    """Parse an HTTP ``Retry-After`` header into seconds.

    Handles both supported forms: a delta in seconds (e.g. ``"120"``) and an
    HTTP-date (e.g. ``"Wed, 21 Oct 2025 07:28:00 GMT"``). Returns ``None`` when
    the header is absent or unparseable. Negative results are clamped to 0.
    """
    if headers is None:
        return None
    try:
        raw = headers.get("Retry-After")
    except AttributeError:
        return None
    if raw is None:
        return None
    raw = str(raw).strip()
    if not raw:
        return None
    # Delta-seconds form.
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    # HTTP-date form.
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def penalize_from_response(
    gate: RateLimitGate, response: Any, *, default: float | None = None
) -> float:
    """Apply a penalty driven by an HTTP response's ``Retry-After`` header.

    Returns the applied delay (seconds). ``response`` is any object with a
    ``headers`` mapping. Invalid / missing headers fall back to ``default``.
    """
    headers = getattr(response, "headers", None) if response is not None else None
    retry_after = parse_retry_after_header(headers)
    if retry_after is None:
        retry_after = default
    return gate.penalize(retry_after)
