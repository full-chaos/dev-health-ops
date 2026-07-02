"""Canonical LaunchDarkly client for flags + audit-log fetches (CHAOS-2761).

This is the ``providers/launchdarkly/`` migration target for the flag and
audit-log fetch logic that historically lived in the frozen
``connectors/launchdarkly.py`` (``LaunchDarklyConnector``). AGENTS.md bans new
code under ``connectors/``, and actuals instrumentation requires a client that
owns a ``UsageRecorder`` (CHAOS-2754) -- so this module ports the connector's
request/retry/pagination logic verbatim (behavior parity: same retry
semantics, same canonical ``dev_health_ops.exceptions.RateLimitException`` +
``RateLimitSignal`` construction the legacy connector already used) and adds
the recorder the frozen connector could never carry.

``connectors/launchdarkly.py`` is left in place, unused by the sync path, but
still imported by the admin credentials "test connection" endpoint
(``api/admin/routers/credentials.py::_test_launchdarkly_connection``), which
mirrors the same raw/legacy-client pattern already used there for Jira and
Linear connectivity checks -- that path never flows through a sync unit or
budget estimation, so it carries no actuals-instrumentation gap.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

import httpx

from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.providers.usage import UsageRecorder
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal

logger = logging.getLogger(__name__)


def _response_host(response: httpx.Response) -> str | None:
    """Best-effort host for the responding LaunchDarkly instance."""
    host = getattr(getattr(response, "url", None), "host", None)
    return host if isinstance(host, str) and host else None


_BASE_URL = "https://app.launchdarkly.com/api/v2"

# LaunchDarkly hard-caps the audit-log `limit` at 20 entries per request; full
# history is assembled by following the `_links.next` cursor across pages.
_AUDIT_LOG_PAGE_SIZE = 20
_API_V2_PREFIX = "/api/v2"

# Response headers worth logging/recording to attribute a 429 or diagnose
# throttling (never the token/Authorization header).
_DIAGNOSTIC_HEADER_NAMES = (
    "x-ratelimit-route-remaining",
    "x-ratelimit-reset",
    "retry-after",
)


def _diagnostic_headers(headers: object) -> dict[str, str]:
    get_items = getattr(headers, "items", None)
    if get_items is None:
        return {}
    lowered = {str(k).lower(): str(v) for k, v in get_items()}
    return {name: lowered[name] for name in _DIAGNOSTIC_HEADER_NAMES if name in lowered}


def _parse_rate_limit_remaining(response: httpx.Response) -> int | None:
    """Extract remaining rate-limit budget from LD response headers."""
    value = response.headers.get("X-RateLimit-Route-Remaining")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_retry_after(response: httpx.Response) -> float | None:
    """Extract Retry-After seconds from a 429 response."""
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(1.0, float(value))
    except (TypeError, ValueError):
        return None


def _raise_for_status(response: httpx.Response) -> None:
    """Translate HTTP error codes into connector exceptions.

    Kept byte-for-byte in step with ``connectors.launchdarkly._raise_for_status``
    and ``providers.launchdarkly.code_refs._raise_for_status`` (parity pinned by
    ``tests/test_rate_limit_signal.py::test_launchdarkly_403_is_authentication_error``).
    """
    status = response.status_code
    if status == 401:
        raise AuthenticationException("LaunchDarkly authentication failed")
    if status == 403:
        # Permission/feature-disabled: non-retryable, matching the
        # GitHub/GitLab convention where a permission 403 is an auth error
        # rather than a retryable APIException.
        raise AuthenticationException(f"LaunchDarkly forbidden: {response.text}")
    if status == 429:
        retry_after = _parse_retry_after(response)
        raise RateLimitException(
            "LaunchDarkly rate limit exceeded",
            retry_after_seconds=retry_after,
            signal=RateLimitSignal(
                provider="launchdarkly",
                host=_response_host(response),
                dimension=BudgetDimension.REST_CORE,
                retry_after_seconds=retry_after,
                # LaunchDarkly reports its reset window as epoch MILLISECONDS.
                reset_at=RateLimitSignal.reset_at_from_epoch_millis(
                    response.headers.get("X-RateLimit-Reset")
                ),
                reason="primary",
            ),
        )
    if status == 404:
        raise APIException(f"LaunchDarkly resource not found: {response.url}")
    if status >= 500:
        raise APIException(f"LaunchDarkly server error: {status} - {response.text}")
    if status >= 400:
        raise APIException(f"LaunchDarkly API error: {status} - {response.text}")


class LaunchDarklyClient:
    """Async canonical client for the LaunchDarkly REST API v2 (flags + audit log).

    :param api_key: LaunchDarkly API access token.
    :param project_key: Default project key (can be overridden per call).
    :param base_url: API base URL (override for testing / private instances).
    :param timeout: Request timeout in seconds.
    :param max_retries: Maximum retry attempts on 429 / 5xx errors.
    """

    def __init__(
        self,
        api_key: str,
        project_key: str | None = None,
        base_url: str = _BASE_URL,
        timeout: int = 30,
        max_retries: int = 5,
    ) -> None:
        self.api_key = api_key
        self.default_project_key = project_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

        # Deferred import mirrors providers/github/client.py + providers/jira/
        # client.py: budget.py is the source of truth for the route-family
        # vocabulary, imported lazily to avoid a module-load-order cycle.
        from dev_health_ops.providers.launchdarkly.budget import (
            LAUNCHDARKLY_USAGE_RESOLVER,
        )

        self._usage = UsageRecorder(resolver=LAUNCHDARKLY_USAGE_RESOLVER)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": self.api_key,
                    "Content-Type": "application/json",
                },
                timeout=self.timeout,
            )
        return self._client

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Execute an HTTP request with retry on 429 / 5xx.

        Every completed HTTP round trip -- including retried 429/5xx attempts
        -- is recorded as one real request via ``_record_rest_usage`` (CHAOS-2754
        contract: actuals are real request counts, never abstract units).
        """
        client = await self._get_client()
        last_exc: Exception | None = None
        delay = 1.0

        for attempt in range(self.max_retries):
            try:
                response = await client.request(method, path, params=params)
                self._record_rest_usage(
                    f"{method} {path}",
                    headers=response.headers,
                    status=response.status_code,
                )

                remaining = _parse_rate_limit_remaining(response)
                if remaining is not None and remaining < 5:
                    logger.warning(
                        "LaunchDarkly rate-limit budget low: %d remaining",
                        remaining,
                    )

                if response.status_code == 429 or response.status_code >= 500:
                    retry_after = _parse_retry_after(response) or delay
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            "LaunchDarkly %d on %s (attempt %d/%d), retrying in %.1fs",
                            response.status_code,
                            sanitize_for_log(path),
                            attempt + 1,
                            self.max_retries,
                            retry_after,
                        )
                        await asyncio.sleep(retry_after)
                        delay = min(delay * 2, 60.0)
                        continue
                    _raise_for_status(response)

                _raise_for_status(response)
                return response.json()

            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    logger.warning(
                        "LaunchDarkly request to %s failed (attempt %d/%d): %s",
                        sanitize_for_log(path),
                        attempt + 1,
                        self.max_retries,
                        exc,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 60.0)
                    continue
                raise APIException(f"LaunchDarkly request failed: {exc}") from exc

        if last_exc:
            raise APIException(
                f"LaunchDarkly request failed after {self.max_retries} attempts"
            ) from last_exc
        raise APIException("LaunchDarkly request failed: unknown error")

    # ------------------------------------------------------------------
    # Usage recording (CHAOS-2754 / CHAOS-2761)
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
        remaining = safe_headers.get("x-ratelimit-route-remaining")
        reset = safe_headers.get("x-ratelimit-reset")
        retry_after = safe_headers.get("retry-after")
        if remaining is not None:
            rate_limit["remaining"] = remaining
        if reset is not None:
            rate_limit["reset"] = reset
        if retry_after is not None:
            rate_limit["retry_after"] = retry_after
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

    async def get_flags(self, project_key: str | None = None) -> list[dict]:
        """Fetch all feature flags for a project, paginating through all pages.

        :param project_key: LD project key; falls back to ``self.default_project_key``.
        :returns: List of raw flag dicts from the LD API.
        """
        key = project_key or self.default_project_key
        if not key:
            raise ValueError(
                "project_key is required (pass it or set default_project_key)"
            )

        all_items: list[dict] = []
        offset = 0
        limit = 50
        while True:
            data = await self._request(
                "GET", f"/flags/{key}", params={"limit": limit, "offset": offset}
            )
            items = data.get("items", [])
            all_items.extend(items)
            total = data.get("totalCount", len(all_items))
            if len(all_items) >= total or len(items) < limit:
                break
            offset += limit
        logger.info(
            "Fetched %d flags for project %s", len(all_items), sanitize_for_log(key)
        )
        return all_items

    async def get_audit_log(
        self,
        since: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Fetch audit log entries, paginating to assemble full history.

        LaunchDarkly caps the audit-log endpoint at 20 entries per request, so
        this pages via the ``_links.next`` cursor until the log is exhausted or
        ``limit`` total entries have been collected.

        :param since: Only return entries occurring after this timestamp.
        :param limit: Maximum total entries to return across all pages.
        :returns: List of raw audit-log entry dicts.
        """
        max_total = max(0, int(limit))
        if max_total == 0:
            return []

        params: dict[str, Any] = {"limit": _AUDIT_LOG_PAGE_SIZE}
        if since is not None:
            # LD expects epoch milliseconds for date filters.
            params["after"] = int(since.timestamp() * 1000)

        all_items: list[dict] = []
        # Bound the page count defensively so a misbehaving cursor cannot loop
        # forever; each page yields at most _AUDIT_LOG_PAGE_SIZE entries.
        max_pages = max_total // _AUDIT_LOG_PAGE_SIZE + 2
        data = await self._request("GET", "/auditlog", params=params)
        for _ in range(max_pages):
            items = data.get("items", [])
            if not items:
                break
            all_items.extend(items)
            if len(all_items) >= max_total:
                break
            href = ((data.get("_links") or {}).get("next") or {}).get("href")
            if not href:
                break
            # base_url already includes /api/v2; strip that prefix so a relative
            # next-href resolves against base_url without duplicating it.
            if href.startswith("http"):
                next_path = href
            elif href.startswith(_API_V2_PREFIX):
                next_path = href[len(_API_V2_PREFIX) :]
            else:
                next_path = href
            data = await self._request("GET", next_path)

        result = all_items[:max_total]
        logger.info("Fetched %d audit log entries", len(result))
        return result

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> LaunchDarklyClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
