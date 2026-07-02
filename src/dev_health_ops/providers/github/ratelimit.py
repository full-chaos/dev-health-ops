"""Shared GitHub 403 triage (CHAOS-2773 CS1).

Extracted from ``providers/github/client.py::GitHubWorkClient.
_raise_github_exception`` so the REST work client and any future httpx-based
``GitHubCodeClient`` (CS3+) classify a 403 through the SAME function -- no
second copy of the primary/secondary/permission decision. Behavior is
pinned by porting the relevant classification cases from
``tests/test_github_403_observability.py`` (which exercises the frozen
``connectors/github.py`` twin of this same triage) into
``tests/providers/test_github_ratelimit_classifier.py``.

A GitHub 403 is one of three things, distinguished only by response headers
and, when headers are absent, the body's documented wording
(docs/providers/rate-limit-policy.md#github):

- **(a) primary rate limit** -- ``x-ratelimit-remaining: 0``.
- **(b) secondary/abuse limit** -- ``Retry-After`` present, or the body
  carries GitHub's documented ``rate limit`` / ``abuse`` / ``secondary``
  wording.
- **(c) permission / SSO / other 403** -- none of the above; NOT a rate
  limit, so the caller must raise a non-retryable error (this module never
  raises -- it only classifies; the caller decides how to signal "not a rate
  limit" for its own exception hierarchy).
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass

from dev_health_ops.sync.budget_types import BudgetDimension


@dataclass(frozen=True)
class GitHubRateLimitClassification:
    """Result of classifying a GitHub 403 (or any status carrying the same
    rate-limit header vocabulary) via :func:`classify_github_403`."""

    is_rate_limit: bool
    is_primary: bool = False
    dimension: BudgetDimension | None = None
    reason: str | None = None
    retry_after_seconds: float | None = None


_NOT_RATE_LIMITED = GitHubRateLimitClassification(is_rate_limit=False)


def classify_github_403(
    *, headers: Mapping[str, str], message: str
) -> GitHubRateLimitClassification:
    """Classify a GitHub 403 using its diagnostic headers + error message.

    :param headers: lower-cased diagnostic headers (as produced by both
        ``providers/github/client.py::_diagnostic_headers`` and
        ``connectors/utils/graphql.py::safe_github_headers`` -- callers pass
        their own already-filtered dict; this function does not care about
        the token/Authorization header since it never receives it).
    :param message: the error body / message text (checked for the
        documented secondary/abuse wording when no header settles it).
    """
    lowered_message = message.lower()
    is_rate_limit = (
        headers.get("x-ratelimit-remaining") == "0"
        or "retry-after" in headers
        or "rate limit" in lowered_message
        or "abuse" in lowered_message
        or "secondary" in lowered_message
    )
    if not is_rate_limit:
        return _NOT_RATE_LIMITED

    is_primary = headers.get("x-ratelimit-remaining") == "0"
    return GitHubRateLimitClassification(
        is_rate_limit=True,
        is_primary=is_primary,
        dimension=(
            BudgetDimension.REST_CORE
            if is_primary
            else BudgetDimension.SECONDARY_ABUSE_RISK
        ),
        reason="primary" if is_primary else "secondary",
        retry_after_seconds=github_retry_after_seconds(headers),
    )


def github_retry_after_seconds(headers: Mapping[str, str]) -> float | None:
    """Resolve the effective retry delay for a rate-limited GitHub response.

    Prefers the explicit ``retry-after`` header (secondary/abuse limits);
    falls back to deriving from ``x-ratelimit-reset`` (an absolute
    epoch-seconds timestamp, primary limits) -- mirrors
    ``providers/github/client.py``'s pre-extraction ``_retry_after_seconds``
    byte-for-byte.
    """
    retry_after = headers.get("retry-after")
    if retry_after is not None:
        try:
            return float(retry_after)
        except ValueError:
            return None

    reset = headers.get("x-ratelimit-reset")
    if reset is not None:
        try:
            return max(0.0, float(reset) - time.time())
        except ValueError:
            return None
    return None


__all__ = [
    "GitHubRateLimitClassification",
    "classify_github_403",
    "github_retry_after_seconds",
]
