"""Canonical GitLab feature-flags client (CHAOS-2785).

This is the ``providers/gitlab/`` migration target for the project
feature-flags + project-name fetch logic that historically lived on the
frozen ``connectors/gitlab.py`` (``GitLabConnector.get_feature_flags`` /
``GitLabConnector.get_project_name``), riding the un-instrumented
``connectors/utils/rest.py::GitLabRESTClient``. AGENTS.md bans new code under
``connectors/``, and actuals instrumentation requires a client that owns a
``UsageRecorder`` (CHAOS-2754) -- so this module ports the fetch/retry/
pagination logic (behavior parity: 403 stays a non-retryable permission
error, 429 stays a retryable rate limit, pinned by
``tests/test_gitlab_connector.py::TestGitLabFeatureFlags403``) and adds the
recorder the frozen connector could never carry.

Mirrors ``providers/launchdarkly/client.py`` (CHAOS-2761) in shape: a single
httpx.AsyncClient owned per instance, one retry loop per physical HTTP round
trip (every attempt -- including retried 429/5xx attempts -- is recorded as
one real request), and the canonical ``dev_health_ops.exceptions.
RateLimitException`` + ``RateLimitSignal`` construction. The epic-wide shared
REST core for GitLab/GitHub/Jira/Linear (CHAOS-2773 CS1) does not exist yet;
this is built standalone in the LaunchDarkly style and will be folded into
that shared core once it lands.

``connectors/gitlab.py`` is left in place, unused by the feature-flags sync
path, but still backs the frozen GitLab code-dataset fetches (commits, files,
blame, CI/CD, tests, deployments, security) -- migrating those is a much
larger canonical-provider effort tracked separately under CHAOS-2773 CS17.
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
from typing import Any

import httpx

from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.providers._ratelimit import (
    gitlab_403_is_rate_limited,
    gitlab_resolve_retry_after_seconds,
)
from dev_health_ops.providers.usage import UsageRecorder
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://gitlab.com"
_API_V4_PREFIX = "/api/v4"
_DEFAULT_PER_PAGE = 100

# Defensive backstop against a misbehaving/looping pagination cursor -- no
# GitLab project should plausibly have this many pages of feature flags.
# Mirrors the audit-log page bound in providers/launchdarkly/client.py.
_MAX_PAGES = 1000

# GitLab's REST rate-limit headers (RateLimit-*, not X-RateLimit-* like
# GitHub/LaunchDarkly) -- see providers/gitlab/client.py
# _maybe_raise_gitlab_rate_limit and docs/providers/rate-limit-policy.md#gitlab.
_DIAGNOSTIC_HEADER_NAMES = (
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
    "retry-after",
)


def _response_host(response: httpx.Response) -> str | None:
    """Best-effort host for the responding GitLab instance (self-managed or
    gitlab.com)."""
    host = getattr(getattr(response, "url", None), "host", None)
    return host if isinstance(host, str) and host else None


def _diagnostic_headers(headers: object) -> dict[str, str]:
    get_items = getattr(headers, "items", None)
    if get_items is None:
        return {}
    lowered = {str(k).lower(): str(v) for k, v in get_items()}
    return {name: lowered[name] for name in _DIAGNOSTIC_HEADER_NAMES if name in lowered}


def _raise_for_status(response: httpx.Response) -> None:
    """Translate HTTP error codes into connector exceptions.

    Parity pin: a plain 403 (Feature Flags disabled for the project, or the
    token lacks Developer+ access) is **non-retryable** -- GitLab's
    documented rate limit is 429 (ops#919) -- so it raises
    ``AuthenticationException`` immediately instead of spending the retry
    budget on an unfixable permission error. Mirrors
    ``connectors/gitlab.py::GitLabConnector.get_feature_flags``'s handling,
    pinned by ``tests/test_gitlab_connector.py::TestGitLabFeatureFlags403``.

    A **header-qualified** 403 (carrying ``Retry-After`` or
    ``RateLimit-Remaining: 0``) is the documented exception: some
    self-managed instances front a throttled request with 403 instead of
    429. That case classifies as a retryable ``RateLimitException`` instead,
    mirroring ``providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit``
    (``GitLabWorkClient``'s classifier) via the shared
    ``providers._ratelimit.gitlab_403_is_rate_limited`` predicate -- checked
    *before* the non-retryable branch below.

    Both the 429 and header-qualified-403 branches resolve the retry delay
    via ``providers._ratelimit.gitlab_resolve_retry_after_seconds`` -- the
    SAME ``Retry-After`` (HTTP-date-aware) + ``RateLimit-Reset`` fallback
    ``GitLabWorkClient``'s classifier uses -- so a self-hosted instance's
    advertised cooldown is honored instead of falling through to a shorter
    caller-local default (``workers/sync_units.py`` plans the worker
    deferral's re-enqueue delay directly from ``retry_after_seconds``).
    """
    status = response.status_code
    if status == 401:
        raise AuthenticationException("GitLab authentication failed")
    if status == 403:
        if gitlab_403_is_rate_limited(response.headers):
            retry_after = gitlab_resolve_retry_after_seconds(response.headers)
            raise RateLimitException(
                "GitLab rate limit exceeded (403 carrying rate-limit headers)",
                retry_after_seconds=retry_after,
                signal=RateLimitSignal(
                    provider="gitlab",
                    host=_response_host(response),
                    dimension=BudgetDimension.REST_CORE,
                    retry_after_seconds=retry_after,
                    reset_at=RateLimitSignal.reset_at_from_epoch_seconds(
                        response.headers.get("RateLimit-Reset")
                    ),
                    # 429 is the documented quota limit; a header-qualified
                    # 403 is the softer/secondary signal -- mirrors
                    # providers/gitlab/client.py's primary/secondary split.
                    reason="secondary",
                ),
            )
        raise AuthenticationException(
            "GitLab feature flags forbidden (403): the Feature Flags feature "
            "is disabled for this project or the token lacks the required "
            f"scope/Developer role. {response.text}"
        )
    if status == 429:
        retry_after = gitlab_resolve_retry_after_seconds(response.headers)
        raise RateLimitException(
            "GitLab rate limit exceeded",
            retry_after_seconds=retry_after,
            signal=RateLimitSignal(
                provider="gitlab",
                host=_response_host(response),
                dimension=BudgetDimension.REST_CORE,
                retry_after_seconds=retry_after,
                # GitLab reports its reset window as epoch SECONDS (unlike
                # LaunchDarkly's epoch milliseconds).
                reset_at=RateLimitSignal.reset_at_from_epoch_seconds(
                    response.headers.get("RateLimit-Reset")
                ),
                reason="primary",
            ),
        )
    if status == 404:
        raise APIException(f"GitLab resource not found: {response.url}")
    if status >= 500:
        raise APIException(f"GitLab server error: {status} - {response.text}")
    if status >= 400:
        raise APIException(f"GitLab API error: {status} - {response.text}")


class GitLabFeatureFlagsClient:
    """Async canonical client for GitLab project feature flags (CHAOS-2785).

    :param private_token: GitLab personal/project access token.
    :param base_url: GitLab instance URL (self-hosted support: pass the
        org's configured GitLab URL; defaults to gitlab.com).
    :param timeout: Request timeout in seconds.
    :param max_retries: Maximum retry attempts on 429 / 5xx errors.
    :param per_page: Default page size for list endpoints.
    """

    def __init__(
        self,
        *,
        private_token: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: int = 15,
        max_retries: int = 5,
        per_page: int = _DEFAULT_PER_PAGE,
    ) -> None:
        self.private_token = private_token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.per_page = per_page
        self._client: httpx.AsyncClient | None = None

        # Deferred import mirrors providers/launchdarkly/client.py: budget.py
        # is the source of truth for the route-family vocabulary, imported
        # lazily to avoid a module-load-order cycle. GITLAB_USAGE_RESOLVER
        # already declares a "project" family matching "/projects/" (used by
        # GitLabWorkClient), which is also the route_family the existing
        # GitLabBudgetEstimator reserves for the FEATURE_FLAGS dataset -- no
        # registry changes needed here.
        from dev_health_ops.providers.gitlab.budget import GITLAB_USAGE_RESOLVER

        self._usage = UsageRecorder(resolver=GITLAB_USAGE_RESOLVER)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=f"{self.base_url}{_API_V4_PREFIX}",
                headers={"PRIVATE-TOKEN": self.private_token},
                timeout=self.timeout,
            )
        return self._client

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Execute an HTTP request with retry on 429 / 5xx / header-qualified 403.

        Every completed HTTP round trip -- including retried attempts -- is
        recorded as one real request via ``_record_rest_usage`` (CHAOS-2754
        contract: actuals are real request counts, never abstract units).

        A 403 carrying rate-limit headers (``Retry-After`` or
        ``RateLimit-Remaining: 0``) is treated the same as a 429 here --
        retried in place with backoff, honoring ``Retry-After`` -- rather
        than escalating to ``RateLimitException`` on the first attempt. A
        *plain* 403 (no rate-limit headers) is never eligible for retry; it
        falls straight through to ``_raise_for_status``'s non-retryable
        ``AuthenticationException`` branch.
        """
        client = await self._get_client()
        delay = 1.0
        last_exc: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                response = await client.request(method, path, params=params)
                self._record_rest_usage(
                    f"{method} {path}",
                    headers=response.headers,
                    status=response.status_code,
                )

                is_retryable_status = (
                    response.status_code == 429
                    or response.status_code >= 500
                    or (
                        response.status_code == 403
                        and gitlab_403_is_rate_limited(response.headers)
                    )
                )
                if is_retryable_status:
                    if attempt < self.max_retries - 1:
                        wait_seconds = (
                            gitlab_resolve_retry_after_seconds(response.headers)
                            or delay
                        )
                        logger.warning(
                            "GitLab %d on %s (attempt %d/%d), retrying in %.1fs",
                            response.status_code,
                            sanitize_for_log(path),
                            attempt + 1,
                            self.max_retries,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        delay = min(delay * 2, 60.0)
                        continue

                _raise_for_status(response)
                return response

            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    logger.warning(
                        "GitLab request to %s failed (attempt %d/%d): %s",
                        sanitize_for_log(path),
                        attempt + 1,
                        self.max_retries,
                        exc,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 60.0)
                    continue
                raise APIException(f"GitLab request failed: {exc}") from exc

        if last_exc:
            raise APIException(
                f"GitLab request failed after {self.max_retries} attempts"
            ) from last_exc
        raise APIException("GitLab request failed: unknown error")

    # ------------------------------------------------------------------
    # Usage recording (CHAOS-2754 / CHAOS-2785)
    # ------------------------------------------------------------------

    def _record_usage_observation(
        self,
        *,
        transport: str,
        operation: str,
        headers: dict[str, str],
        rate_limit: dict[str, Any],
        status: int | None = None,
    ) -> None:
        # Aggregation/keying by route_family lives in the shared recorder
        # (CHAOS-2754); this client only owns the header extraction below.
        self._usage.record(
            transport=transport,
            operation=operation,
            headers=headers,
            rate_limit=rate_limit,
            status=status,
        )

    def _record_rest_usage(
        self,
        operation: str,
        *,
        headers: object | None = None,
        status: int | None = None,
    ) -> None:
        safe_headers = _diagnostic_headers(headers or {})
        rate_limit: dict[str, Any] = {}
        for source, target in {
            "ratelimit-remaining": "remaining",
            "ratelimit-reset": "reset",
            "ratelimit-limit": "limit",
            "retry-after": "retry_after",
        }.items():
            value = safe_headers.get(source)
            if value is not None:
                rate_limit[target] = value
        self._record_usage_observation(
            transport="rest",
            operation=operation,
            headers=safe_headers,
            rate_limit=rate_limit,
            status=status,
        )

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return self._usage.drain()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def _next_page(
        response: httpx.Response, current_page: int, item_count: int, per_page: int
    ) -> int | None:
        """Resolve the next page number from GitLab's ``X-Next-Page`` header,
        falling back to an item-count heuristic when the header is absent
        (e.g. against a mocked/older GitLab instance)."""
        next_page_header = response.headers.get("X-Next-Page")
        if next_page_header:
            try:
                return int(next_page_header)
            except ValueError:
                return None
        if item_count < per_page:
            return None
        return current_page + 1

    async def get_feature_flags(
        self,
        project_id_or_path: int | str,
        *,
        per_page: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch feature flags for a GitLab project via the management API.

        Paginates until GitLab returns an empty page (or no next page), with
        a defensive ``_MAX_PAGES`` cap against a misbehaving cursor.
        """
        encoded_project = urllib.parse.quote(str(project_id_or_path), safe="")
        endpoint = f"/projects/{encoded_project}/feature_flags"
        effective_per_page = per_page or self.per_page

        flags: list[dict[str, Any]] = []
        page: int | None = 1
        pages_fetched = 0
        while page is not None:
            if pages_fetched >= _MAX_PAGES:
                logger.warning(
                    "GitLab feature flags pagination hit the %d-page cap for "
                    "project %s; results may be truncated",
                    _MAX_PAGES,
                    sanitize_for_log(str(project_id_or_path)),
                )
                break
            response = await self._request(
                "GET",
                endpoint,
                params={"page": page, "per_page": effective_per_page},
            )
            pages_fetched += 1
            batch = response.json()
            if not isinstance(batch, list):
                raise APIException(
                    f"Unexpected GitLab feature flags response: {type(batch)!r}"
                )
            if not batch:
                break
            flags.extend(batch)
            page = self._next_page(response, page, len(batch), effective_per_page)

        logger.info(
            "Fetched %d GitLab feature flags for project %s",
            len(flags),
            sanitize_for_log(str(project_id_or_path)),
        )
        return flags

    async def get_project_name(self, project_id_or_path: int | str) -> str:
        """Return the canonical path for a GitLab project."""
        encoded_project = urllib.parse.quote(str(project_id_or_path), safe="")
        response = await self._request("GET", f"/projects/{encoded_project}")
        data = response.json()
        if not isinstance(data, dict):
            return str(project_id_or_path)
        return str(
            data.get("path_with_namespace") or data.get("path") or project_id_or_path
        )

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> GitLabFeatureFlagsClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
