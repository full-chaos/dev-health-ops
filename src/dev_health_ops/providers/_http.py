"""Shared instrumented REST transport core (CHAOS-2773 CS1).

``InstrumentedRESTCore`` is the epic's foundation: the one httpx transport
primitive every canonical code client (``GitHubCodeClient``,
``GitLabCodeClient``, and any provider added later) composes rather than
inherits from. It mirrors the shape already proven by
``providers/launchdarkly/client.py`` (the sanctioned template, #1126) and
``providers/gitlab/feature_flags.py`` (the #1142 "will be folded into the
shared core once it lands" precedent) -- one owned ``UsageRecorder``, one
``UsageRecorder.record`` per PHYSICAL HTTP round trip including retried
attempts, ``Retry-After``-aware backoff, and the canonical
``dev_health_ops.exceptions.RateLimitException`` + CHAOS-2753
``RateLimitSignal`` on exhaustion.

**Composition, not inheritance.** This module owns transport mechanics only:
requests, retries, pagination, usage recording, and a *default* status-code
classification good enough to exercise in isolation. It deliberately does
NOT know GitHub's 403 triage or GitLab's header-qualified-403 semantics --
those stay in ``providers/github/ratelimit.py`` / ``providers/gitlab/
ratelimit.py`` and are wired in by a future code client via the
``is_retryable_status`` / ``classify_error`` extension points below (CS3+).
Credential resolution also stays entirely above this module: callers build
their own ``httpx`` headers (bearer token, ``PRIVATE-TOKEN``, ...) and pass
them in; this core never touches auth.

**Base-URL joining** reuses the existing ``GitHubAuth.base_url`` /
``GitLabAuth.base_url`` string fields (``providers/github/client.py`` /
``providers/gitlab/client.py``) -- callers pass the raw string those
dataclasses already carry into :func:`github_rest_base_url` /
:func:`gitlab_rest_base_url` below, so this module never imports those
provider client modules (that would invert the dependency direction: they
will import ``_http`` in CS3+, not the reverse).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

from dev_health_ops.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.providers._ratelimit import resolve_retry_after_seconds
from dev_health_ops.providers.usage import OperationResolver, UsageRecorder
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Base-URL joining (GHE / self-hosted GitLab)
# ---------------------------------------------------------------------------

GITHUB_DEFAULT_BASE_URL = "https://api.github.com"
GITLAB_DEFAULT_BASE_URL = "https://gitlab.com"
_GITLAB_API_V4_PREFIX = "/api/v4"


def github_rest_base_url(base_url: str | None) -> str:
    """Resolve the GitHub REST API base URL from ``GitHubAuth.base_url``.

    ``None`` (or empty) resolves to ``api.github.com``. A GitHub Enterprise
    value is the FULL REST base including the ``/api/v3`` suffix (e.g.
    ``https://ghe.example.com/api/v3``) -- joined AS-IS with no path
    rewriting, mirroring ``providers/github/app_auth.py::
    _installation_access_tokens_url`` and ``connectors/github.py``'s
    ``Github(base_url=...)`` construction (both existing GHE precedents).
    """
    return str(base_url or GITHUB_DEFAULT_BASE_URL).rstrip("/")


def gitlab_rest_base_url(base_url: str | None) -> str:
    """Resolve the GitLab REST API base URL from ``GitLabAuth.base_url``.

    ``GitLabAuth.base_url`` is the instance HOST only (``https://gitlab.com``
    or a self-hosted URL, e.g. ``https://gitlab.example.com``) -- python-gitlab
    and ``providers/gitlab/feature_flags.py`` both append ``/api/v4``
    themselves; this mirrors that join exactly (``connectors/gitlab.py``'s
    ``api_url = f"{url}/api/v4"``).
    """
    return (
        f"{str(base_url or GITLAB_DEFAULT_BASE_URL).rstrip('/')}{_GITLAB_API_V4_PREFIX}"
    )


# ---------------------------------------------------------------------------
# Diagnostic header extraction (provider-configurable)
# ---------------------------------------------------------------------------

# Per-provider diagnostic header allowlists. These MUST stay in exact parity
# with the sets the existing work-client recorders preserve
# (providers/github/client.py::_DIAGNOSTIC_HEADER_NAMES and
# providers/gitlab/client.py::_DIAGNOSTIC_HEADER_NAMES) so a code client
# built on this core emits the SAME ``latest_headers`` shape as the existing
# recorders -- request IDs and permission diagnostics included. Parity is
# pinned by tests/providers/test_http_core.py::TestDiagnosticHeaderParity
# (codex review MED-1 on PR #1149). Code clients (CS3+) pass the matching
# tuple as ``diagnostic_header_names``.
GITHUB_DIAGNOSTIC_HEADER_NAMES = (
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "x-ratelimit-used",
    "x-ratelimit-resource",
    "retry-after",
    "x-github-request-id",
    "x-accepted-github-permissions",
)

GITLAB_DIAGNOSTIC_HEADER_NAMES = (
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
    "retry-after",
    "x-request-id",
    "x-runtime",
)

# Generic default: the union of both providers' rate-limit/diagnostic sets --
# safe when a caller has not passed a provider-specific tuple yet.
_DEFAULT_DIAGNOSTIC_HEADER_NAMES = tuple(
    dict.fromkeys(GITHUB_DIAGNOSTIC_HEADER_NAMES + GITLAB_DIAGNOSTIC_HEADER_NAMES)
)


def _diagnostic_headers(
    headers: httpx.Headers, names: tuple[str, ...]
) -> dict[str, str]:
    lowered = {str(k).lower(): str(v) for k, v in headers.items()}
    return {name: lowered[name] for name in names if name in lowered}


def _default_rate_limit_fields(safe_headers: dict[str, str]) -> dict[str, Any]:
    rate_limit: dict[str, Any] = {}
    remaining = safe_headers.get("x-ratelimit-remaining") or safe_headers.get(
        "ratelimit-remaining"
    )
    reset = safe_headers.get("x-ratelimit-reset") or safe_headers.get("ratelimit-reset")
    limit = safe_headers.get("x-ratelimit-limit") or safe_headers.get("ratelimit-limit")
    used = safe_headers.get("x-ratelimit-used")
    resource = safe_headers.get("x-ratelimit-resource")
    retry_after = safe_headers.get("retry-after")
    if remaining is not None:
        rate_limit["remaining"] = remaining
    if reset is not None:
        rate_limit["reset"] = reset
    if limit is not None:
        rate_limit["limit"] = limit
    if used is not None:
        rate_limit["used"] = used
    if resource is not None:
        rate_limit["resource"] = resource
    if retry_after is not None:
        rate_limit["retry_after"] = retry_after
    return rate_limit


def _response_host(response: httpx.Response) -> str | None:
    host = getattr(getattr(response, "url", None), "host", None)
    return host if isinstance(host, str) and host else None


# ---------------------------------------------------------------------------
# Default retry / classification policy
# ---------------------------------------------------------------------------

_DEFAULT_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
_DEFAULT_MAX_RETRIES = 5
_DEFAULT_INITIAL_BACKOFF_SECONDS = 1.0
_DEFAULT_MAX_BACKOFF_SECONDS = 60.0
_DEFAULT_TIMEOUT_SECONDS = 30.0
_DEFAULT_RESET_HEADER_NAME = "X-RateLimit-Reset"

IsRetryableStatus = Callable[[httpx.Response], bool]
ResolveRetryAfter = Callable[[httpx.Response], float | None]
ClassifyError = Callable[[httpx.Response, str], None]


def _default_is_retryable_status(response: httpx.Response) -> bool:
    return response.status_code in _DEFAULT_RETRYABLE_STATUSES


@dataclass
class InstrumentedRESTCore:
    """Provider-agnostic, instrumented httpx REST transport (CHAOS-2773 CS1).

    One instance is owned by one code client, built per sync unit -- same
    lifecycle contract as ``UsageRecorder`` (no cross-unit/cross-org state).

    :param base_url: Already-joined REST API base (see
        :func:`github_rest_base_url` / :func:`gitlab_rest_base_url`).
    :param provider: Provider slug (``"github"`` / ``"gitlab"``) stamped onto
        every ``RateLimitSignal`` this core raises.
    :param resolver: The provider's ``OperationResolver``
        (``GITHUB_USAGE_RESOLVER`` / ``GITLAB_USAGE_RESOLVER``) -- this core
        builds and owns its own :class:`UsageRecorder` from it, mirroring
        every existing canonical client's ``__init__``.
    :param headers: Default headers sent with every request (auth token,
        ``Accept``, ...). Credential resolution happens ABOVE this module;
        this is just the resulting header dict.
    :param reset_header_name: Header carrying the epoch-seconds rate-limit
        reset used by the DEFAULT 429 classification AND the default
        retry-delay derivation
        (``X-RateLimit-Reset`` for GitHub, ``RateLimit-Reset`` for GitLab).
    :param diagnostic_header_names: Response-header allowlist recorded onto
        usage observations (never the token/Authorization header). Code
        clients pass their provider's tuple
        (:data:`GITHUB_DIAGNOSTIC_HEADER_NAMES` /
        :data:`GITLAB_DIAGNOSTIC_HEADER_NAMES`) so the recorded
        ``latest_headers`` shape matches the existing work-client recorders
        exactly; the default is the safe union of both.
    :param is_retryable_status: Predicate deciding whether a response status
        should be retried in-place. Defaults to ``{429, 500, 502, 503, 504}``;
        a GitLab code client overrides this to also retry a
        header-qualified 403 (mirrors ``providers/gitlab/feature_flags.py``).
    :param resolve_retry_after: Resolves the backoff delay for a retryable
        response. Default (``None``) uses the shared
        :func:`providers._ratelimit.resolve_retry_after_seconds` -- the
        #1142 delay resolution generalized over ``reset_header_name``:
        ``Retry-After`` when present (delta-seconds or HTTP-date), else
        derived from the provider's epoch-seconds reset header, so a
        reset-only 429 still carries the server's real window instead of
        falling back to the worker's short default (codex HIGH on PR #1149).
    :param classify_error: Optional hook invoked with ``(response, operation)``
        for any TERMINAL non-2xx response (retries exhausted, or a
        non-retryable status) BEFORE the built-in default classification. May
        raise a domain-specific exception (e.g. GitHub's 403 triage); if it
        returns without raising, the default classification below still runs.
    """

    base_url: str
    provider: str
    resolver: OperationResolver
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float = _DEFAULT_TIMEOUT_SECONDS
    max_retries: int = _DEFAULT_MAX_RETRIES
    initial_backoff_seconds: float = _DEFAULT_INITIAL_BACKOFF_SECONDS
    max_backoff_seconds: float = _DEFAULT_MAX_BACKOFF_SECONDS
    reset_header_name: str = _DEFAULT_RESET_HEADER_NAME
    diagnostic_header_names: tuple[str, ...] = _DEFAULT_DIAGNOSTIC_HEADER_NAMES
    is_retryable_status: IsRetryableStatus = field(default=_default_is_retryable_status)
    resolve_retry_after: ResolveRetryAfter | None = None
    classify_error: ClassifyError | None = None
    transport: httpx.AsyncBaseTransport | None = None

    def __post_init__(self) -> None:
        self._usage = UsageRecorder(resolver=self.resolver)
        self._client: httpx.AsyncClient | None = None
        self._bare_client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self.headers,
                timeout=self.timeout,
                transport=self.transport,
            )
        return self._client

    async def _get_bare_client(self) -> httpx.AsyncClient:
        """Second client with NO caller default headers (no ``Authorization``,
        no API-version pins) for :meth:`request_unauthenticated`. httpx sends
        a client's default headers even on absolute-URL requests and rejects
        ``headers={'Authorization': None}``, so a separate headerless client
        is the only clean way to issue a deliberately unauthenticated hop
        (codex re-pass MED on PR #1149). Shares the transport/timeout so
        tests and mocks see both hops through one seam."""
        if self._bare_client is None or self._bare_client.is_closed:
            self._bare_client = httpx.AsyncClient(
                timeout=self.timeout,
                transport=self.transport,
            )
        return self._bare_client

    # ------------------------------------------------------------------
    # Core request loop
    # ------------------------------------------------------------------

    async def request(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json: Any | None = None,
        raw_redirect: bool = False,
    ) -> httpx.Response:
        """Issue one LOGICAL request, retrying transparently in place.

        Every PHYSICAL HTTP round trip -- including retried 429/5xx attempts
        -- records exactly one usage observation via ``_record_usage``
        (CHAOS-2754 contract: actuals are real request counts, never
        abstract units). ``operation`` is the caller-authored label (often a
        path TEMPLATE, e.g. ``"cicd:GET /repos/{o}/{r}/actions/runs"``,
        distinct from the literal ``path`` sent over the wire) used both for
        usage-recorder resolution and for diagnostic messages.

        **Redirect policy (codex MED-2 on PR #1149).** The underlying client
        never follows redirects (httpx default), so an un-handled 3xx would
        otherwise masquerade as a success with no hop accounting and blow up
        later at ``.json()``. Default: any 3xx is a terminal
        ``APIException`` naming the redirect target. ``raw_redirect=True``
        opts a single call into receiving the 3xx response itself (still
        recorded as one physical round trip, never retried) -- the first
        half of the two-hop artifact pattern below.

        **Two-hop unauthenticated-follow pattern (CS5's artifact-zip
        contract, connectors/github.py::download_artifact_zip).** GitHub's
        artifact download 302s to a pre-signed blob/CDN URL that MUST be
        fetched without forwarding ``Authorization``. httpx sends the
        client's default headers even on absolute-URL requests, so the
        follow-up cannot go through :meth:`request` -- use
        :meth:`request_unauthenticated` for the second hop, which issues it
        with NO client default headers while still recording the physical
        attempt::

            first = await core.request(
                "GET", f"/repos/{o}/{r}/actions/artifacts/{i}/zip",
                operation="tests:GET artifact zip", raw_redirect=True,
            )
            if first.status_code in (301, 302, 303, 307, 308):
                blob = await core.request_unauthenticated(
                    first.headers["Location"],
                    operation="tests:GET artifact zip follow",
                )
        """
        client = await self._get_client()
        delay = self.initial_backoff_seconds
        last_network_exc: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                response = await client.request(
                    method, path, params=params, headers=headers, json=json
                )
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_network_exc = exc
                if attempt < self.max_retries - 1:
                    logger.warning(
                        "%s request to %s failed (attempt %d/%d): %s",
                        self.provider,
                        operation,
                        attempt + 1,
                        self.max_retries,
                        exc,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self.max_backoff_seconds)
                    continue
                raise APIException(
                    f"{self.provider} request failed on {operation}: {exc}"
                ) from exc

            self._record_response_usage(response, operation=operation)

            if response.status_code < 300:
                return response

            if response.status_code < 400:
                if raw_redirect:
                    return response
                raise APIException(
                    f"{self.provider} unexpected redirect on {operation}: HTTP "
                    f"{response.status_code} -> "
                    f"{response.headers.get('Location', '<no Location header>')}; "
                    "the instrumented core does not follow redirects (pass "
                    "raw_redirect=True to receive the redirect response and "
                    "handle Location manually)"
                )

            if self.is_retryable_status(response) and attempt < self.max_retries - 1:
                retry_after = self._resolve_retry_after(response) or delay
                logger.warning(
                    "%s %d on %s (attempt %d/%d), retrying in %.1fs",
                    self.provider,
                    response.status_code,
                    operation,
                    attempt + 1,
                    self.max_retries,
                    retry_after,
                )
                await asyncio.sleep(retry_after)
                delay = min(delay * 2, self.max_backoff_seconds)
                continue

            self._raise_for_status(response, operation=operation)

        if last_network_exc is not None:
            raise APIException(
                f"{self.provider} request failed after {self.max_retries} attempts "
                f"on {operation}"
            ) from last_network_exc
        raise APIException(
            f"{self.provider} request failed on {operation}: unknown error"
        )

    async def request_unauthenticated(
        self,
        url: str,
        *,
        operation: str,
        method: str = "GET",
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Issue one deliberately UNAUTHENTICATED request, still recorded.

        The second hop of the two-hop redirect pattern (see :meth:`request`'s
        redirect-policy docs): after ``raw_redirect=True`` hands back a 3xx,
        this follows the absolute ``Location`` with NO client default headers
        -- no ``Authorization``, no API-version pins, nothing beyond what the
        caller passes explicitly in ``headers`` -- so a pre-signed blob/CDN
        URL never sees the provider token (codex re-pass MED on PR #1149;
        contract per connectors/github.py::download_artifact_zip).

        The physical attempt is recorded through the SAME ``UsageRecorder``
        as :meth:`request` (the hop is real provider-triggered traffic), and
        the response is returned AS-IS: no retry loop and no status
        classification, because the pre-signed host is a different error
        domain from the provider API -- e.g. the artifact contract treats
        404/410 as convenience-empty, which the code client (CS5) decides,
        not this transport. Network-level failures raise ``APIException``
        like the main path.

        :param url: ABSOLUTE URL (a redirect ``Location``); relative paths
            are rejected -- they would silently resolve against nothing and
            a relative Location should be followed via :meth:`request`,
            which keeps auth, instead.
        """
        if not url.lower().startswith(("http://", "https://")):
            raise ValueError(
                "request_unauthenticated requires an absolute http(s) URL "
                f"(got {url!r}); relative redirect targets stay on the "
                "authenticated client via request()"
            )
        client = await self._get_bare_client()
        try:
            response = await client.request(method, url, headers=headers)
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            raise APIException(
                f"{self.provider} unauthenticated request failed on {operation}: {exc}"
            ) from exc
        self._record_response_usage(response, operation=operation)
        return response

    def _resolve_retry_after(self, response: httpx.Response) -> float | None:
        """Resolve the server-advertised retry delay for a response.

        Uses the caller-supplied ``resolve_retry_after`` override when set;
        otherwise the shared ``providers/_ratelimit.py`` resolution:
        ``Retry-After`` first, falling back to deriving the delay from the
        provider's epoch-seconds reset header (``reset_header_name``). The
        fallback matters for terminal 429s: the worker deferral path plans
        ``not_before`` from ``exc.retry_after_seconds``, NOT from
        ``signal.reset_at`` -- a reset-only 429 that left
        ``retry_after_seconds=None`` would wake the unit on the 60s default
        instead of the provider's real reset window (codex HIGH, PR #1149).
        """
        if self.resolve_retry_after is not None:
            return self.resolve_retry_after(response)
        return resolve_retry_after_seconds(
            response.headers, reset_header_name=self.reset_header_name
        )

    def _raise_for_status(self, response: httpx.Response, *, operation: str) -> None:
        """Terminal classification for a non-2xx response (retries exhausted,
        or a status :attr:`is_retryable_status` never considered retryable).

        ``classify_error`` (if set) runs FIRST and may raise a
        provider-specific exception (GitHub 403 triage, GitLab
        header-qualified 403, ...); returning normally falls through to the
        generic default below.
        """
        if self.classify_error is not None:
            self.classify_error(response, operation)

        status = response.status_code
        if status == 401:
            raise AuthenticationException(
                f"{self.provider} authentication failed on {operation}"
            )
        if status == 403:
            raise AuthenticationException(
                f"{self.provider} forbidden on {operation}: {response.text}"
            )
        if status == 404:
            raise NotFoundException(
                f"{self.provider} resource not found on {operation}: {response.url}"
            )
        if status == 429:
            retry_after = self._resolve_retry_after(response)
            raise RateLimitException(
                f"{self.provider} rate limit exceeded on {operation}",
                retry_after_seconds=retry_after,
                signal=RateLimitSignal(
                    provider=self.provider,
                    host=_response_host(response),
                    # integration_id / route_family are enriched at the
                    # worker boundary (CHAOS-2753) -- clients never set them.
                    dimension=BudgetDimension.REST_CORE,
                    retry_after_seconds=retry_after,
                    reset_at=RateLimitSignal.reset_at_from_epoch_seconds(
                        response.headers.get(self.reset_header_name)
                    ),
                    reason="primary",
                ),
            )
        if status >= 500:
            raise APIException(
                f"{self.provider} server error on {operation}: {status} - {response.text}"
            )
        raise APIException(
            f"{self.provider} API error on {operation}: {status} - {response.text}"
        )

    # ------------------------------------------------------------------
    # Paginators (hard page caps -- defensive against a misbehaving/looping
    # cursor; mirrors providers/gitlab/feature_flags.py's _MAX_PAGES bound)
    # ------------------------------------------------------------------

    async def paginate_link_header(
        self,
        path: str,
        *,
        operation: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        data_key: str | None = None,
        max_pages: int = 100,
    ) -> list[Any]:
        """Paginate a GitHub-style endpoint via the RFC 5988 ``Link`` header.

        Follows ``rel="next"`` until absent or ``max_pages`` is hit. GitHub's
        ``next`` link is an ABSOLUTE url (including a GHE host), which httpx
        honors directly regardless of the client's configured ``base_url``.

        :param data_key: When the endpoint wraps its list in an envelope
            (e.g. GitHub Actions' ``{"workflow_runs": [...]}"``), the key to
            extract; ``None`` when the JSON payload IS the list.
        """
        items: list[Any] = []
        next_url: str | None = path
        next_params: dict[str, Any] | None = dict(params or {}) if params else None
        pages = 0
        while next_url is not None:
            if pages >= max_pages:
                logger.warning(
                    "%s pagination hit the %d-page cap for %s",
                    self.provider,
                    max_pages,
                    operation,
                )
                break
            response = await self.request(
                "GET",
                next_url,
                operation=operation,
                params=next_params,
                headers=headers,
            )
            pages += 1
            payload = response.json()
            page_items = payload.get(data_key, []) if data_key else payload
            if not isinstance(page_items, list):
                raise APIException(
                    f"Unexpected paginated response for {operation}: {type(page_items)!r}"
                )
            items.extend(page_items)
            next_url = _parse_link_header_next(response.headers.get("Link"))
            # The next URL already carries its own query string; only the
            # FIRST request applies caller-supplied params.
            next_params = None
        return items

    async def paginate_page_param(
        self,
        path: str,
        *,
        operation: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        per_page: int = 100,
        max_pages: int = 100,
    ) -> list[Any]:
        """Paginate a GitLab-style endpoint via ``page``/``per_page`` query
        params, following ``X-Next-Page`` when present (falling back to an
        item-count heuristic when it is absent, e.g. an older/mocked
        instance) -- mirrors ``providers/_base.py::BasePipelineAdapter.
        _paginate`` and ``providers/gitlab/feature_flags.py::_next_page``.
        """
        items: list[Any] = []
        page: int | None = 1
        pages = 0
        while page is not None:
            if pages >= max_pages:
                logger.warning(
                    "%s pagination hit the %d-page cap for %s",
                    self.provider,
                    max_pages,
                    operation,
                )
                break
            current_params = dict(params or {})
            current_params["page"] = page
            current_params.setdefault("per_page", per_page)
            response = await self.request(
                "GET",
                path,
                operation=operation,
                params=current_params,
                headers=headers,
            )
            pages += 1
            payload = response.json()
            if not isinstance(payload, list):
                raise APIException(
                    f"Unexpected paginated response for {operation}: {type(payload)!r}"
                )
            if not payload:
                break
            items.extend(payload)
            page = self._next_page_param(response, page, len(payload), per_page)
        return items

    @staticmethod
    def _next_page_param(
        response: httpx.Response, current_page: int, item_count: int, per_page: int
    ) -> int | None:
        next_page_header = response.headers.get("X-Next-Page")
        if next_page_header:
            try:
                return int(next_page_header)
            except ValueError:
                return None
        if item_count < per_page:
            return None
        return current_page + 1

    # ------------------------------------------------------------------
    # Usage recording (CHAOS-2754)
    # ------------------------------------------------------------------

    def _record_response_usage(
        self, response: httpx.Response, *, operation: str
    ) -> None:
        safe_headers = _diagnostic_headers(
            response.headers, self.diagnostic_header_names
        )
        self._usage.record(
            transport="rest",
            operation=operation,
            headers=safe_headers,
            rate_limit=_default_rate_limit_fields(safe_headers),
            status=response.status_code,
        )

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return self._usage.drain()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
        if self._bare_client is not None and not self._bare_client.is_closed:
            await self._bare_client.aclose()
            self._bare_client = None

    async def __aenter__(self) -> InstrumentedRESTCore:
        await self._get_client()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()


def _parse_link_header_next(link_header: str | None) -> str | None:
    """Extract the ``rel="next"`` URL from an RFC 5988 ``Link`` header.

    Returns ``None`` when the header is absent or carries no ``next`` rel
    (the last page) -- GitHub omits the header entirely on a single-page
    result and drops the ``next`` segment on the final page of a multi-page
    result.
    """
    if not link_header:
        return None
    for segment in link_header.split(","):
        parts = segment.split(";")
        if len(parts) < 2:
            continue
        url_part = parts[0].strip()
        if not (url_part.startswith("<") and url_part.endswith(">")):
            continue
        rel_is_next = any(param.strip().lower() == 'rel="next"' for param in parts[1:])
        if rel_is_next:
            return url_part[1:-1]
    return None


__all__ = [
    "GITHUB_DEFAULT_BASE_URL",
    "GITHUB_DIAGNOSTIC_HEADER_NAMES",
    "GITLAB_DEFAULT_BASE_URL",
    "GITLAB_DIAGNOSTIC_HEADER_NAMES",
    "InstrumentedRESTCore",
    "github_rest_base_url",
    "gitlab_rest_base_url",
]
