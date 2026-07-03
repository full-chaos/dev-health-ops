"""GitHub instrumented httpx code client (CHAOS-2773 CS3 pathfinder).

Ports the "security" code-dataset family -- Dependabot alerts, code-scanning
alerts, security advisories -- off the frozen ``connectors/github.py`` REST
methods (``get_dependabot_alerts`` / ``get_code_scanning_alerts`` /
``get_security_advisories`` and their shared ``_get_security_alert_page``
pager) onto ``providers/_http.py::InstrumentedRESTCore``.

This is the epic's PATHFINDER client: the shape here (one owned
``InstrumentedRESTCore`` configured with GitHub's diagnostic headers and 403
triage, ``"<family>:"``-prefixed operation labels for the CS1 resolver
short-circuit, degrade-to-empty on a permission/SSO 403 or 404 for these
OPTIONAL endpoints, ``RateLimitException`` on a rate-limited 403 or an
exhausted 429) is the template later changesets (CS4 deployments, CS5
cicd/tests, ...) copy for their own GitHub code-dataset families -- each
adding sibling methods to this same client labeled with their own family
prefix, never a second ``InstrumentedRESTCore`` construction pattern.

Behavior parity with the connector is pinned by
``tests/providers/test_github_code_client_security.py``.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from math import ceil
from typing import Any

import httpx

from dev_health_ops.connectors.models import SecurityAlertData
from dev_health_ops.exceptions import (
    AuthenticationException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.providers._http import (
    GITHUB_DIAGNOSTIC_HEADER_NAMES,
    InstrumentedRESTCore,
    _default_is_retryable_status,
    github_rest_base_url,
)
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.ratelimit import (
    classify_github_403,
    github_retry_after_seconds,
)
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal

logger = logging.getLogger(__name__)

# CS1 resolver explicit-prefix short-circuit (providers/usage.py::
# OperationResolver): every operation this client labels resolves DIRECTLY to
# the matching route family (providers/github/budget.py's
# GITHUB_USAGE_ROUTE_FAMILIES entry), bypassing the substring marker scan.
SECURITY_ROUTE_FAMILY = "security"
DEPLOYMENTS_ROUTE_FAMILY = "deployments"
_GITHUB_DEPLOYMENTS_PER_PAGE = 100


@dataclass(frozen=True)
class GitHubReleaseData:
    tag_name: str | None


@dataclass(frozen=True)
class GitHubDeploymentData:
    deployment_id: str
    state: str | None
    environment: str | None
    created_at: datetime | None
    sha: str | None
    ref: str | None
    tag: str | None
    tag_name: str | None
    payload: Mapping[str, Any] | None


def _lowered_github_headers(response: httpx.Response) -> dict[str, str]:
    """Lower-cased diagnostic headers -- the shape both ``classify_github_403``
    and ``github_retry_after_seconds`` expect (never the Authorization/token
    header, which is not in ``GITHUB_DIAGNOSTIC_HEADER_NAMES``)."""
    lowered = {str(k).lower(): str(v) for k, v in response.headers.items()}
    return {
        name: lowered[name]
        for name in GITHUB_DIAGNOSTIC_HEADER_NAMES
        if name in lowered
    }


def _response_host(response: httpx.Response) -> str | None:
    host = getattr(getattr(response, "url", None), "host", None)
    return host if isinstance(host, str) and host else None


def _classify_github_code_client_error(
    response: httpx.Response, operation: str
) -> None:
    """``InstrumentedRESTCore.classify_error`` hook: triage a 403 through the
    shared ``classify_github_403`` (the SAME classifier
    ``GitHubWorkClient._raise_github_exception`` uses -- no second copy of the
    primary/secondary/permission decision).

    A rate-limited 403 (primary or secondary/abuse) raises the canonical
    ``RateLimitException`` here, short-circuiting the core's generic
    classification. A permission/SSO/other 403 -- or any other status --
    returns normally, falling through to the core's default (401/403/404/429/
    5xx) classification, which raises ``AuthenticationException`` for that
    403 case -- exactly matching ``connectors/github.py``'s "return None ->
    caller decides" contract.
    """
    status = response.status_code
    if status == 401:
        logger.warning(
            "GitHub security endpoint 401 on %s headers=%s -- degrading to "
            "empty (check token validity)",
            operation,
            _lowered_github_headers(response),
        )
        return
    if status != 403:
        return
    headers = _lowered_github_headers(response)
    classification = classify_github_403(headers=headers, message=response.text)
    if not classification.is_rate_limit:
        logger.warning(
            "GitHub security endpoint non-rate-limit 403 on %s headers=%s -- "
            "degrading to empty (check token scope / SSO authorization)",
            operation,
            headers,
        )
        return
    logger.warning(
        "GitHub rate limit (403) on %s headers=%s reason=%s",
        operation,
        headers,
        classification.reason,
    )
    raise RateLimitException(
        f"GitHub rate limit (403) on {operation} (headers={headers})",
        retry_after_seconds=classification.retry_after_seconds,
        signal=RateLimitSignal(
            provider="github",
            host=_response_host(response),
            dimension=classification.dimension,
            retry_after_seconds=classification.retry_after_seconds,
            reset_at=RateLimitSignal.reset_at_from_epoch_seconds(
                headers.get("x-ratelimit-reset")
            ),
            reason=classification.reason,
            request_id=headers.get("x-github-request-id"),
        ),
    )


def _resolve_github_retry_after(response: httpx.Response) -> float | None:
    """``InstrumentedRESTCore.resolve_retry_after`` hook: wraps
    ``github_retry_after_seconds`` (which takes a lower-cased header mapping)
    to match the core's ``Callable[[httpx.Response], float | None]`` shape."""
    return github_retry_after_seconds(_lowered_github_headers(response))


def _github_is_retryable_status(response: httpx.Response) -> bool:
    """``InstrumentedRESTCore.is_retryable_status`` hook: extend the default
    (429 / 5xx) to ALSO retry a RATE-LIMITED 403 -- GitHub secondary/abuse
    limits arrive as a 403 (often with ``Retry-After``), which the default
    predicate would treat as terminal. Restores the frozen connector's
    ``retry_with_backoff(exceptions=(RateLimitException, APIException))``
    parity: a rate-limited 403 gets backed-off retries before the terminal
    ``RateLimitException``. A plain permission/SSO 403 is NOT a rate limit and
    stays non-retryable (it degrades to empty for these optional endpoints)."""
    if response.status_code == 403:
        return classify_github_403(
            headers=_lowered_github_headers(response), message=response.text
        ).is_rate_limit
    return _default_is_retryable_status(response)


def _parse_alert_datetime(value: object) -> datetime | None:
    """Byte-for-byte port of ``connectors/github.py::
    GitHubConnector._parse_github_datetime`` -- a naive/aware ISO-8601 string
    (GitHub always sends a ``Z``-suffixed UTC timestamp) -> ``datetime``."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.debug("Failed to parse GitHub datetime: %s", value)
        return None


def _dependabot_alert_from_item(item: dict[str, Any]) -> SecurityAlertData:
    advisory = item.get("security_advisory") or {}
    dependency = item.get("dependency") or {}
    package = dependency.get("package") or {}
    return SecurityAlertData(
        alert_id=f"dependabot:{item['number']}",
        source="dependabot",
        severity=advisory.get("severity"),
        state=item["state"],
        package_name=package.get("name"),
        cve_id=advisory.get("cve_id"),
        url=item.get("html_url"),
        title=advisory.get("summary"),
        description=advisory.get("description"),
        created_at=_parse_alert_datetime(item.get("created_at")),
        fixed_at=_parse_alert_datetime(item.get("fixed_at")),
        dismissed_at=_parse_alert_datetime(item.get("dismissed_at")),
    )


def _code_scanning_alert_from_item(item: dict[str, Any]) -> SecurityAlertData:
    rule = item.get("rule") or {}
    most_recent_instance = item.get("most_recent_instance") or {}
    message = most_recent_instance.get("message") or {}
    return SecurityAlertData(
        alert_id=f"code_scanning:{item['number']}",
        source="code_scanning",
        severity=rule.get("severity"),
        state=item["state"],
        package_name=None,
        cve_id=None,
        url=item.get("html_url"),
        title=rule.get("description"),
        description=message.get("text"),
        created_at=_parse_alert_datetime(item.get("created_at")),
        fixed_at=None,
        dismissed_at=_parse_alert_datetime(item.get("dismissed_at")),
    )


def _security_advisory_from_item(item: dict[str, Any]) -> SecurityAlertData:
    return SecurityAlertData(
        alert_id=f"advisory:{item['ghsa_id']}",
        source="advisory",
        severity=item.get("severity"),
        state=item.get("state"),
        package_name=None,
        cve_id=item.get("cve_id"),
        url=item.get("html_url"),
        title=item.get("summary"),
        description=item.get("description"),
        created_at=_parse_alert_datetime(item.get("created_at")),
        fixed_at=None,
        dismissed_at=None,
    )


def _release_from_item(item: Mapping[str, Any]) -> GitHubReleaseData:
    tag_name = item.get("tag_name")
    return GitHubReleaseData(tag_name=str(tag_name) if tag_name is not None else None)


def _deployment_from_item(item: Mapping[str, Any]) -> GitHubDeploymentData:
    payload = item.get("payload")
    deployment_id = item.get("id")
    sha = item.get("sha")
    ref = item.get("ref")
    tag = item.get("tag")
    tag_name = item.get("tag_name")
    state = item.get("state") or item.get("status")
    environment = item.get("environment")
    return GitHubDeploymentData(
        deployment_id=str(deployment_id or ""),
        state=str(state) if state is not None else None,
        environment=str(environment) if environment is not None else None,
        created_at=_parse_alert_datetime(item.get("created_at")),
        sha=str(sha) if sha is not None else None,
        ref=str(ref) if ref is not None else None,
        tag=str(tag) if tag is not None else None,
        tag_name=str(tag_name) if tag_name is not None else None,
        payload=payload if isinstance(payload, Mapping) else None,
    )


def _pull_request_merged_at(item: Mapping[str, Any]) -> datetime | None:
    return _parse_alert_datetime(item.get("merged_at"))


def _pull_request_number(item: Mapping[str, Any]) -> int | None:
    raw_number = item.get("number")
    try:
        return int(raw_number) if raw_number is not None else None
    except (TypeError, ValueError):
        return None


def _choose_deployment_pull_request(
    pulls: list[Mapping[str, Any]], sha: str
) -> tuple[int | None, datetime | None]:
    merged = [pull for pull in pulls if _pull_request_merged_at(pull) is not None]
    direct = [pull for pull in merged if pull.get("merge_commit_sha") == sha]
    chosen = (
        direct[0]
        if direct
        else (merged[0] if merged else (pulls[0] if pulls else None))
    )
    if chosen is None:
        return None, None
    return _pull_request_number(chosen), _pull_request_merged_at(chosen)


def _page_cap_for_limit(
    limit: int | None, per_page: int = _GITHUB_DEPLOYMENTS_PER_PAGE
) -> int:
    if limit is None:
        return 100
    return max(1, ceil(limit / per_page))


class GitHubCodeClient:
    """Instrumented httpx client for GitHub code-dataset families
    (CHAOS-2773 CS3+). CS3 exposes the ``security`` family only.

    :param auth: Token + optional GHE base URL. Mirrors
        ``providers/github/client.py::GitHubAuth`` -- callers that already
        resolved a GitHub App installation token onto a connector (this
        client's only construction path today, via
        ``processors/github.py::_github_code_client_from_connector``) pass
        that resolved plain token here; this client does not itself refresh
        short-lived App tokens (CS3 scope; the connector already did that).
    :param transport: Optional ``httpx.AsyncBaseTransport`` override, passed
        straight through to the owned ``InstrumentedRESTCore`` -- the seam
        parity tests use to mock GitHub's REST API (``httpx.MockTransport``),
        never live network (offline gate).
    """

    def __init__(
        self, *, auth: GitHubAuth, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        if not auth.token:
            raise ValueError("GitHubCodeClient requires a resolved token")
        self.auth = auth

        # Deferred import mirrors providers/github/client.py: budget.py is the
        # source of truth for the route-family vocabulary, imported lazily to
        # avoid a module-load-order cycle.
        from dev_health_ops.providers.github.budget import GITHUB_USAGE_RESOLVER

        self._core = InstrumentedRESTCore(
            base_url=github_rest_base_url(auth.base_url),
            provider="github",
            resolver=GITHUB_USAGE_RESOLVER,
            headers={
                "Authorization": f"token {auth.token}",
                "Accept": "application/vnd.github+json",
            },
            diagnostic_header_names=GITHUB_DIAGNOSTIC_HEADER_NAMES,
            classify_error=_classify_github_code_client_error,
            resolve_retry_after=_resolve_github_retry_after,
            is_retryable_status=_github_is_retryable_status,
            transport=transport,
        )

    async def get_dependabot_alerts(
        self,
        owner: str,
        repo: str,
        *,
        state: str = "open",
        max_alerts: int | None = None,
    ) -> list[SecurityAlertData]:
        """GET /repos/{owner}/{repo}/dependabot/alerts (paginated)."""
        return await self._get_security_alerts(
            owner,
            repo,
            endpoint="dependabot/alerts",
            params={"state": state, "per_page": 100},
            max_alerts=max_alerts,
            build=_dependabot_alert_from_item,
        )

    async def get_code_scanning_alerts(
        self,
        owner: str,
        repo: str,
        *,
        state: str = "open",
        max_alerts: int | None = None,
    ) -> list[SecurityAlertData]:
        """GET /repos/{owner}/{repo}/code-scanning/alerts (paginated)."""
        return await self._get_security_alerts(
            owner,
            repo,
            endpoint="code-scanning/alerts",
            params={"state": state, "per_page": 100},
            max_alerts=max_alerts,
            build=_code_scanning_alert_from_item,
        )

    async def get_security_advisories(
        self,
        owner: str,
        repo: str,
        *,
        state: str | None = None,
        max_alerts: int | None = None,
    ) -> list[SecurityAlertData]:
        """GET /repos/{owner}/{repo}/security-advisories (paginated)."""
        params: dict[str, Any] = {"per_page": 100}
        if state is not None:
            params["state"] = state
        return await self._get_security_alerts(
            owner,
            repo,
            endpoint="security-advisories",
            params=params,
            max_alerts=max_alerts,
            build=_security_advisory_from_item,
        )

    async def get_deployment_releases(
        self,
        owner: str,
        repo: str,
        *,
        max_releases: int | None = None,
    ) -> list[GitHubReleaseData]:
        operation = f"{DEPLOYMENTS_ROUTE_FAMILY}:GET /repos/{owner}/{repo}/releases"
        items = await self._core.paginate_link_header(
            f"/repos/{owner}/{repo}/releases",
            operation=operation,
            params={"per_page": _GITHUB_DEPLOYMENTS_PER_PAGE},
            max_pages=_page_cap_for_limit(max_releases),
        )
        if max_releases is not None:
            items = items[:max_releases]
        return [_release_from_item(item) for item in items if isinstance(item, Mapping)]

    async def get_deployments(
        self,
        owner: str,
        repo: str,
        *,
        max_deployments: int | None = None,
    ) -> list[GitHubDeploymentData]:
        operation = f"{DEPLOYMENTS_ROUTE_FAMILY}:GET /repos/{owner}/{repo}/deployments"
        items = await self._core.paginate_link_header(
            f"/repos/{owner}/{repo}/deployments",
            operation=operation,
            params={"per_page": _GITHUB_DEPLOYMENTS_PER_PAGE},
            max_pages=_page_cap_for_limit(max_deployments),
        )
        if max_deployments is not None:
            items = items[:max_deployments]
        return [
            _deployment_from_item(item) for item in items if isinstance(item, Mapping)
        ]

    async def get_deployment_pull_request(
        self,
        owner: str,
        repo: str,
        sha: str | None,
    ) -> tuple[int | None, datetime | None]:
        if not sha:
            return None, None
        operation = (
            f"{DEPLOYMENTS_ROUTE_FAMILY}:GET /repos/{owner}/{repo}/commits/{sha}/pulls"
        )
        try:
            response = await self._core.request(
                "GET",
                f"/repos/{owner}/{repo}/commits/{sha}/pulls",
                operation=operation,
                params={"per_page": 10},
                headers={"Accept": "application/vnd.github.groot-preview+json"},
            )
            pulls = response.json()
        except Exception as exc:
            logger.debug("Failed PR lookup for deployed commit %s: %s", sha, exc)
            return None, None
        if not isinstance(pulls, list):
            logger.debug("Unexpected PR lookup payload for deployed commit %s", sha)
            return None, None
        pull_items = [pull for pull in pulls if isinstance(pull, Mapping)]
        return _choose_deployment_pull_request(pull_items, sha)

    async def _get_security_alerts(
        self,
        owner: str,
        repo: str,
        *,
        endpoint: str,
        params: dict[str, Any],
        max_alerts: int | None,
        build: Any,
    ) -> list[SecurityAlertData]:
        """Shared pager for the three security endpoints -- ports
        ``connectors/github.py::_get_security_alert_page`` onto
        ``InstrumentedRESTCore.paginate_link_header`` (Link-header
        ``rel="next"`` pagination; the first request applies ``params``,
        follow-up requests use the absolute next URL as-is, matching the
        connector byte-for-byte).

        A permission/SSO 403 (``AuthenticationException``, not a rate limit
        per ``classify_github_403``) or a 404 (``NotFoundException``) on
        these OPTIONAL endpoints degrades to an EMPTY list -- discarding any
        items already gathered from earlier pages in this call, exactly like
        the connector's ``return []`` (the feature is likely disabled, or the
        token lacks the scope). A rate-limited 403 or an exhausted 429 raises
        ``RateLimitException`` and is NOT caught here -- it propagates to the
        caller (``processors/github.py``'s per-endpoint degrade-and-log loop
        decides from there, unchanged from today).
        """
        operation = f"{SECURITY_ROUTE_FAMILY}:GET /repos/{owner}/{repo}/{endpoint}"
        try:
            items = await self._core.paginate_link_header(
                f"/repos/{owner}/{repo}/{endpoint}",
                operation=operation,
                params=params,
            )
        except (AuthenticationException, NotFoundException):
            return []
        alerts = [build(item) for item in items]
        if max_alerts is not None:
            alerts = alerts[:max_alerts]
        return alerts

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return self._core.drain_usage_observations()

    async def close(self) -> None:
        await self._core.close()

    async def __aenter__(self) -> GitHubCodeClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()


__all__ = [
    "DEPLOYMENTS_ROUTE_FAMILY",
    "GitHubCodeClient",
    "GitHubDeploymentData",
    "GitHubReleaseData",
    "SECURITY_ROUTE_FAMILY",
]
