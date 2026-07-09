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

import time
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


def gitlab_403_is_rate_limited(headers: Any) -> bool:
    """Return ``True`` when a GitLab 403 actually carries rate-limit signal.

    GitLab returns 403 both for permission/feature-disabled errors
    (non-retryable) and, less commonly, for a throttled request that a
    self-managed instance's proxy tier fronts with 403 instead of 429 --
    distinguishable only by the presence of rate-limit headers (``Retry-
    After``, or ``RateLimit-Remaining: 0``); see
    ``docs/providers/rate-limit-policy.md#gitlab``.

    Mirrors ``providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit``'s
    403 qualification (the canonical ``GitLabWorkClient``'s classifier). That
    copy operates on a python-gitlab ``GitlabError`` exception's
    ``response_code``/``response_headers`` attributes, so it can't be called
    directly from an httpx-based client; this header-only extraction is the
    shared predicate every GitLab provider client should use instead of
    re-deriving the same boolean check a third time.

    :param headers: any object exposing a ``.get(name)`` mapping interface --
        an ``httpx.Headers``, a plain ``dict``, or python-gitlab's
        ``response_headers``.
    """
    if headers is None:
        return False
    try:
        remaining = headers.get("RateLimit-Remaining")
        retry_after_raw = headers.get("Retry-After")
    except AttributeError:
        return False
    return (
        parse_retry_after_header(headers) is not None
        or str(remaining) == "0"
        or retry_after_raw is not None
    )


def resolve_retry_after_seconds(
    headers: Any, *, reset_header_name: str
) -> float | None:
    """Resolve the effective retry delay from rate-limit response headers.

    Prefers ``Retry-After`` via :func:`parse_retry_after_header` (handles
    both delta-seconds and HTTP-date forms). When that header is absent or
    unparseable, derives the delay from ``reset_header_name`` (an absolute
    epoch-seconds timestamp -- ``X-RateLimit-Reset`` for GitHub,
    ``RateLimit-Reset`` for GitLab) instead of leaving the caller with
    ``None`` -- a caller that treats "no Retry-After" as "no signal at all"
    and falls back to its own short default backoff ends up re-hammering a
    still-throttled instance sooner than the server intends. Provider-
    parameterized generalization of the GitLab-specific delay resolution
    shipped in #1142 (see :func:`gitlab_resolve_retry_after_seconds`, which
    now delegates here) so the CHAOS-2773 CS1 shared REST core reuses ONE
    implementation instead of growing a second copy.

    :param headers: any object exposing a ``.get(name)`` mapping interface --
        an ``httpx.Headers``, a plain ``dict``, or python-gitlab's
        ``response_headers``.
    :param reset_header_name: header carrying the provider's epoch-seconds
        rate-limit reset timestamp.
    """
    retry_after = parse_retry_after_header(headers)
    if retry_after is not None:
        return retry_after
    if headers is None:
        return None
    try:
        reset_raw = headers.get(reset_header_name)
    except AttributeError:
        return None
    if reset_raw is None:
        return None
    try:
        return max(0.0, float(reset_raw) - time.time())
    except (TypeError, ValueError):
        return None


def gitlab_resolve_retry_after_seconds(headers: Any) -> float | None:
    """Resolve the effective retry delay for a rate-limited GitLab response.

    Prefers ``Retry-After`` via :func:`parse_retry_after_header` (handles
    both delta-seconds and HTTP-date forms). When that header is absent or
    unparseable, derives the delay from ``RateLimit-Reset`` (an absolute
    epoch-seconds timestamp) instead of leaving the caller with ``None``.

    Mirrors ``providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit``'s
    ``Retry-After`` / ``RateLimit-Reset`` fallback byte-for-byte; extracted
    here so GitLab provider clients other than ``GitLabWorkClient`` derive
    the SAME delay from the SAME headers instead of reimplementing (or
    worse, silently omitting) the ``RateLimit-Reset`` fallback. Thin
    GitLab-pinned wrapper over :func:`resolve_retry_after_seconds`.

    :param headers: any object exposing a ``.get(name)`` mapping interface --
        an ``httpx.Headers``, a plain ``dict``, or python-gitlab's
        ``response_headers``.
    """
    return resolve_retry_after_seconds(headers, reset_header_name="RateLimit-Reset")
