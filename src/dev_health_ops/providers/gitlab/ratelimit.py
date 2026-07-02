"""Shared GitLab rate-limit classifier (CHAOS-2773 CS1).

Consolidates GitLab's 429-primary / header-qualified-403-secondary
classification into ONE call site, built directly on the shared predicate +
delay primitives already merged in ``providers/_ratelimit.py``
(``gitlab_403_is_rate_limited`` / ``gitlab_resolve_retry_after_seconds``,
#1142 -- the #1142 feature-flags client already uses them directly). This
module is what ``providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit``
now DELEGATES to, so exactly one predicate/delay implementation exists for
GitLab (the work client no longer re-derives the same boolean/delay logic a
second time), and it doubles as the classifier any future httpx-based
``GitLabCodeClient`` (CS3+) reuses via
:class:`~dev_health_ops.providers._http.InstrumentedRESTCore`'s
``classify_error`` extension point.

**429 vs. 403 convention** (docs/providers/rate-limit-policy.md#gitlab): 429
is GitLab's documented quota limit and is ALWAYS a rate limit; a 403 is
permission/feature-disabled UNLESS it carries rate-limit headers (some
self-managed instances front a throttled request with 403 instead of 429),
in which case it is treated as the softer "secondary" signal -- mirroring
GitHub's primary/secondary split.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.providers._ratelimit import (
    gitlab_403_is_rate_limited,
    gitlab_resolve_retry_after_seconds,
)
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal


@dataclass(frozen=True)
class GitLabRateLimitClassification:
    """Result of classifying a GitLab response status + headers."""

    is_rate_limit: bool
    reason: str | None = None  # "primary" (429) | "secondary" (header-qualified 403)
    retry_after_seconds: float | None = None


_NOT_RATE_LIMITED = GitLabRateLimitClassification(is_rate_limit=False)


def _header_get(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    try:
        return headers.get(name)
    except AttributeError:
        return None


def classify_gitlab_status(
    *, status: int | None, headers: Any
) -> GitLabRateLimitClassification:
    """Classify a GitLab response status + headers for rate-limit signal.

    :param status: HTTP status code (``response_code`` on a python-gitlab
        ``GitlabError``, or ``response.status_code`` on an httpx response).
    :param headers: any ``.get(name)``-mapping object -- ``httpx.Headers``, a
        plain ``dict``, or python-gitlab's ``response_headers``.
    """
    if status == 429:
        return GitLabRateLimitClassification(
            is_rate_limit=True,
            reason="primary",
            retry_after_seconds=gitlab_resolve_retry_after_seconds(headers),
        )
    if status == 403 and gitlab_403_is_rate_limited(headers):
        return GitLabRateLimitClassification(
            is_rate_limit=True,
            reason="secondary",
            retry_after_seconds=gitlab_resolve_retry_after_seconds(headers),
        )
    return _NOT_RATE_LIMITED


def build_gitlab_rate_limit_exception(
    *,
    status: int | None,
    headers: Any,
    classification: GitLabRateLimitClassification,
    request_id: str | None = None,
) -> RateLimitException:
    """Build the canonical ``RateLimitException`` + ``RateLimitSignal`` for a
    response already classified as rate-limited (call
    :func:`classify_gitlab_status` first; ``classification.is_rate_limit``
    must be ``True``)."""
    return RateLimitException(
        f"GitLab rate limited (HTTP {status})",
        retry_after_seconds=classification.retry_after_seconds,
        signal=RateLimitSignal(
            provider="gitlab",
            dimension=BudgetDimension.REST_CORE,
            retry_after_seconds=classification.retry_after_seconds,
            # GitLab reports its reset window as epoch SECONDS (unlike
            # LaunchDarkly's epoch milliseconds).
            reset_at=RateLimitSignal.reset_at_from_epoch_seconds(
                _header_get(headers, "RateLimit-Reset")
            ),
            reason=classification.reason,
            request_id=request_id,
        ),
    )


def maybe_raise_gitlab_rate_limit(
    *, status: int | None, headers: Any, request_id: str | None = None
) -> None:
    """Raise the canonical GitLab ``RateLimitException`` if rate-limited,
    else return ``None`` (the caller's non-rate-limit handling continues)."""
    classification = classify_gitlab_status(status=status, headers=headers)
    if classification.is_rate_limit:
        raise build_gitlab_rate_limit_exception(
            status=status,
            headers=headers,
            classification=classification,
            request_id=request_id,
        )


__all__ = [
    "GitLabRateLimitClassification",
    "build_gitlab_rate_limit_exception",
    "classify_gitlab_status",
    "maybe_raise_gitlab_rate_limit",
]
