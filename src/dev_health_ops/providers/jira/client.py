from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any, Literal
from urllib.parse import quote, urlparse

import requests
from anyio.to_thread import run_sync
from pydantic import JsonValue

from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
    create_rate_limit_gate,
)
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.providers._ratelimit import (
    parse_retry_after_header,
    penalize_from_response,
)
from dev_health_ops.providers.jira.jsm_models import parse_jsm_native_incident
from dev_health_ops.providers.usage import UsageRecorder
from dev_health_ops.providers.utils import EnvSpec, read_env_spec
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal

logger = logging.getLogger(__name__)

_DIAGNOSTIC_HEADER_NAMES = (
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
    "retry-after",
    "x-request-id",
    "atl-traceid",
)

_MAX_JSM_PAGES = 1_000
_MAX_JSM_ROWS = 100_000
_JSM_INCIDENT_FIELDS = (
    "id",
    "key",
    "summary",
    "created",
    "updated",
    "resolutiondate",
    "status",
    "priority",
)
_JSM_INCIDENTS_API_ORIGIN = "https://api.atlassian.com"


def _require_jira() -> Any:
    try:
        from jira import JIRA

        return JIRA
    except (
        Exception
    ) as exc:  # pragma: no cover - exercised in docs/runtime, not unit tests
        raise RuntimeError(
            "Jira support requires the 'jira' package. Install dependencies from requirements.txt."
        ) from exc


def _normalize_jira_base_url(value: str) -> str:
    """
    Normalize Jira Cloud base URL to an https:// URL.

    Accepts values like:
    - https://your-org.atlassian.net
    - your-org.atlassian.net
    """
    url = (value or "").strip()
    url = url.rstrip("/")
    if not url:
        return url
    if url.startswith("http://"):
        return "https://" + url[len("http://") :]
    if url.startswith("https://"):
        return url
    return "https://" + url.lstrip("/")


def _diagnostic_headers(headers: object) -> dict[str, str]:
    get_items = getattr(headers, "items", None)
    if get_items is None:
        return {}
    lowered = {str(k).lower(): str(v) for k, v in get_items()}
    return {name: lowered[name] for name in _DIAGNOSTIC_HEADER_NAMES if name in lowered}


def validate_jsm_cloud_origin(value: str) -> str:
    """Return a trusted bare Jira Cloud origin for JSM incident reads only."""
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError as error:
        raise RuntimeError(
            "JSM incident reads require a bare HTTPS *.atlassian.net origin"
        ) from error
    hostname = parsed.hostname
    if (
        parsed.scheme != "https"
        or hostname is None
        or not hostname.casefold().endswith(".atlassian.net")
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.path not in ("", "/")
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError(
            "JSM incident reads require a bare HTTPS *.atlassian.net origin"
        )
    return f"https://{hostname.casefold()}"


@dataclass(frozen=True, slots=True)
class JiraAuth:
    base_url: str
    email: str
    api_token: str
    cloud_id: str | None = None


class JiraClient:
    """
    Small Jira Cloud client wrapper for issue ingestion.

    Uses Jira Cloud REST API and adds:
    - pagination helper
    - shared RateLimitGate-based backoff

    Note: Jira Cloud has removed `GET /rest/api/3/search`; this client uses
    `GET /rest/api/3/search/jql`.
    """

    def __init__(
        self,
        *,
        auth: JiraAuth,
        timeout_seconds: int = 30,
        per_page: int = 100,
        gate: RateLimitGate | None = None,
        org_id: str | None = None,
        max_retries_429: int = 3,
    ) -> None:
        import requests

        self.auth = auth
        self.timeout_seconds = int(timeout_seconds)
        self.per_page = max(1, min(100, int(per_page)))
        host = urlparse(auth.base_url).hostname or "_"
        self.gate = gate or create_rate_limit_gate(
            "jira",
            org_id=org_id,
            host=host,
            config=RateLimitConfig(initial_backoff_seconds=1.0),
        )
        self.max_retries_429 = max(0, int(max_retries_429))

        from dev_health_ops.providers.jira.budget import JIRA_USAGE_RESOLVER

        self._usage = UsageRecorder(resolver=JIRA_USAGE_RESOLVER)

        self.session = requests.Session()
        self.session.auth = (auth.email, auth.api_token)
        self.session.headers.update({"Accept": "application/json"})
        self._jsm_cloud_id: str | None = None

    @classmethod
    def from_env(cls, *, org_id: str | None = None) -> JiraClient:
        env = read_env_spec(
            EnvSpec(
                required={
                    "base_url": "JIRA_BASE_URL",
                    "email": "JIRA_EMAIL",
                    "api_token": "JIRA_API_TOKEN",
                },
                missing_error=(
                    "Jira env vars required: JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN"
                ),
            )
        )
        return cls(
            auth=JiraAuth(
                base_url=_normalize_jira_base_url(str(env["base_url"])),
                email=str(env["email"]),
                api_token=str(env["api_token"]),
            ),
            org_id=org_id,
        )

    def close(self) -> None:
        try:
            self.session.close()
        except Exception as exc:
            logger.debug("Error while closing JiraClient session", exc_info=exc)

    def _url(self, path: str) -> str:
        return f"{self.auth.base_url}{path}"

    def _request_json(
        self,
        *,
        path: str,
        params: Mapping[str, str | int] | None = None,
        method: Literal["GET", "POST"] = "GET",
        json: dict[str, JsonValue] | None = None,
        allow_redirects: bool = True,
        allow_list: bool = False,
    ) -> dict[str, JsonValue]:
        import requests

        url = self._url(path)
        attempts = self.max_retries_429 + 1
        for attempt in range(attempts):
            self.gate.wait_sync()
            try:
                if method == "GET":
                    resp = self.session.get(
                        url,
                        params=params,
                        timeout=self.timeout_seconds,
                        allow_redirects=allow_redirects,
                    )
                else:
                    resp = self.session.post(
                        url,
                        json=json,
                        timeout=self.timeout_seconds,
                        allow_redirects=allow_redirects,
                    )
                self._record_rest_usage(
                    f"{method} {path}", headers=resp.headers, status=resp.status_code
                )
                if not allow_redirects and 300 <= resp.status_code < 400:
                    raise RuntimeError("JSM incident reads do not follow redirects")
                if resp.status_code == 429:
                    retry_after = parse_retry_after_header(resp.headers)
                    applied = penalize_from_response(self.gate, resp)
                    logger.info(
                        "Jira rate limited; backoff %.1fs (HTTP 429, attempt %d/%d)",
                        applied,
                        attempt + 1,
                        attempts,
                    )
                    if attempt + 1 < attempts:
                        continue
                    raise RateLimitException(
                        f"Jira rate limited: giving up after {attempts} attempts (HTTP 429)",
                        retry_after_seconds=retry_after,
                        signal=RateLimitSignal(
                            provider="jira",
                            host=urlparse(self.auth.base_url).hostname,
                            dimension=BudgetDimension.REST_CORE,
                            retry_after_seconds=retry_after,
                            # Jira's X-RateLimit-Reset is an ISO 8601 timestamp
                            # (Atlassian Cloud rate-limiting docs), not epoch
                            # seconds like GitHub/GitLab -- CHAOS-2758 verified
                            # this against the docs (see RateLimitSignal
                            # docstring); Retry-After remains authoritative.
                            reset_at=RateLimitSignal.reset_at_from_iso8601(
                                resp.headers.get("X-RateLimit-Reset")
                            ),
                            reason="primary",
                            request_id=resp.headers.get("X-AREQUESTID"),
                        ),
                    )
                resp.raise_for_status()
                self.gate.reset()
                data = resp.json()
                if isinstance(data, dict):
                    return data
                if allow_list and isinstance(data, list):
                    return {"_legacy_list": data}
                if not isinstance(data, dict):
                    raise RuntimeError("Jira response JSON must be an object")
            except requests.HTTPError as exc:
                logger.debug(
                    "Jira request failed: method=%s path=%s status=%s",
                    method,
                    path,
                    exc.response.status_code if exc.response is not None else "unknown",
                )
                raise
        raise RuntimeError("Jira request retry loop exited without a result")

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
        for source, target in [
            ("x-ratelimit-remaining", "remaining"),
            ("x-ratelimit-reset", "reset"),
            ("x-ratelimit-limit", "limit"),
            ("ratelimit-remaining", "remaining"),
            ("ratelimit-reset", "reset"),
            ("ratelimit-limit", "limit"),
            ("retry-after", "retry_after"),
        ]:
            value = safe_headers.get(source)
            if value is not None:
                rate_limit.setdefault(target, value)
        self._record_usage_observation(
            transport="rest",
            operation=operation,
            headers=safe_headers,
            rate_limit=rate_limit,
            status=status,
        )

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return self._usage.drain()

    def search_issues_page(
        self,
        *,
        jql: str,
        start_at: int,
        max_results: int,
        fields: Iterable[str] | None = None,
        expand: str | None = None,
        next_page_token: str | None = None,
    ) -> Any:
        params: dict[str, Any] = {
            "jql": str(jql),
            "maxResults": int(max_results),
        }
        if next_page_token:
            params["nextPageToken"] = str(next_page_token)
        else:
            params["startAt"] = int(start_at)
        if fields:
            params["fields"] = ",".join([str(f) for f in fields])
        else:
            params["fields"] = "*all"
        if expand:
            params["expand"] = str(expand)
        return self._request_json(path="/rest/api/3/search/jql", params=params)

    def iter_issues(
        self,
        *,
        jql: str,
        fields: Iterable[str] | None = None,
        expand_changelog: bool = True,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Iterate issues matching a JQL query with pagination.

        NOTE: Jira may truncate changelogs on large issues. For full history,
        callers may need to fetch per-issue changelogs separately.
        """
        start_at = 0
        fetched = 0
        expand = "changelog" if expand_changelog else None
        next_page_token: str | None = None

        while True:
            logger.debug(
                "Jira search page startAt=%d maxResults=%d", start_at, self.per_page
            )
            page = self.search_issues_page(
                jql=jql,
                start_at=start_at,
                max_results=self.per_page,
                fields=fields,
                expand=expand,
                next_page_token=next_page_token,
            )
            issues = list((page or {}).get("issues") or [])
            if not issues:
                logger.debug("Jira search complete; fetched=%d", fetched)
                break

            for issue in issues:
                yield issue
                fetched += 1
                if limit is not None and fetched >= int(limit):
                    return

            if (page or {}).get("nextPageToken"):
                next_page_token = str((page or {}).get("nextPageToken"))
            else:
                next_page_token = None
                start_at += len(issues)

            if (page or {}).get("isLast") is True:
                logger.debug("Jira search complete (isLast=true); fetched=%d", fetched)
                break

    def fetch_issue_comments_page(
        self,
        *,
        issue_id_or_key: str,
        start_at: int,
        max_results: int,
    ) -> Any:
        params: dict[str, Any] = {
            "startAt": int(start_at),
            "maxResults": int(max_results),
        }
        return self._request_json(
            path=f"/rest/api/3/issue/{issue_id_or_key}/comment",
            params=params,
        )

    def iter_issue_comments(
        self,
        *,
        issue_id_or_key: str,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        start_at = 0
        fetched = 0

        while True:
            page = self.fetch_issue_comments_page(
                issue_id_or_key=issue_id_or_key,
                start_at=start_at,
                max_results=self.per_page,
            )
            comments = list((page or {}).get("comments") or [])
            if not comments:
                break

            for comment in comments:
                yield comment
                fetched += 1
                if limit is not None and fetched >= int(limit):
                    return

            start_at += len(comments)
            if (page or {}).get("isLast") is True:
                break

    def get_sprint(self, *, sprint_id: str) -> dict[str, Any]:
        return self._request_json(
            path=f"/rest/agile/1.0/sprint/{sprint_id}",
            params={},
        )

    def iter_boards(
        self, *, project_key: str | None = None
    ) -> Iterator[dict[str, Any]]:
        start_at = 0
        while True:
            params: dict[str, Any] = {
                "startAt": start_at,
                "maxResults": self.per_page,
            }
            if project_key:
                params["projectKeyOrId"] = project_key
            page = self._request_json(path="/rest/agile/1.0/board", params=params)
            board_values = page.get("values")
            boards = (
                [board for board in board_values if isinstance(board, dict)]
                if isinstance(board_values, list)
                else []
            )
            if not boards:
                break
            yield from boards
            start_at += len(boards)
            if (page or {}).get("isLast") is True:
                break

    def iter_board_sprints(self, *, board_id: int | str) -> Iterator[dict[str, Any]]:
        start_at = 0
        while True:
            page = self._request_json(
                path=f"/rest/agile/1.0/board/{board_id}/sprint",
                params={"startAt": start_at, "maxResults": self.per_page},
            )
            sprint_values = page.get("values")
            sprints = (
                [sprint for sprint in sprint_values if isinstance(sprint, dict)]
                if isinstance(sprint_values, list)
                else []
            )
            if not sprints:
                break
            yield from sprints
            start_at += len(sprints)
            if (page or {}).get("isLast") is True:
                break

    def get_all_projects(self) -> list[dict[str, Any]]:
        """
        Fetch all visible projects from Jira.
        Uses GET /rest/api/3/project/search for pagination.
        """
        projects: list[dict[str, Any]] = []
        start_at = 0
        max_results = 50

        while True:
            params: dict[str, str | int] = {
                "startAt": start_at,
                "maxResults": max_results,
                "expand": "description,lead",
            }
            # Note: project/search is the modern endpoint, but fallback to project if needed.
            # We'll try project/search first.
            try:
                data = self._request_json(
                    path="/rest/api/3/project/search", params=params
                )
                page = data.get("values", [])
            except requests.HTTPError as error:
                response = error.response
                status_code = response.status_code if response is not None else None
                if status_code not in {404, 405, 410}:
                    raise
                # Fallback to non-paginated (or differently paginated) /project endpoint
                # which usually returns all projects if the list is small, or
                # strictly follows deprecated behavior.
                # Ideally, we stick to /search. If it fails, we might just re-raise.
                logger.warning(
                    "Jira project/search failed, trying /project (may be unpaginated)"
                )
                legacy_response = self._request_json(
                    path="/rest/api/3/project", params={}, allow_list=True
                )
                legacy_projects = legacy_response.get("_legacy_list")
                if not isinstance(legacy_projects, list):
                    raise RuntimeError("Jira legacy project response must be a list")
                return [
                    project for project in legacy_projects if isinstance(project, dict)
                ]

            if not isinstance(page, list):
                raise RuntimeError("Jira project search response values must be a list")
            projects_page = [project for project in page if isinstance(project, dict)]
            if not projects_page:
                break

            projects.extend(projects_page)
            if data.get("isLast"):
                break

            start_at += len(projects_page)
            # Safety break for massive instances if isLast isn't reliable
            if len(projects_page) < max_results:
                break

        return projects

    async def iter_service_desks(self) -> AsyncIterator[str]:
        """Enumerate all Jira Service Management service project keys."""
        await self._ensure_jsm_cloud_identity()
        start = 0
        pages = 0
        rows_seen = 0
        while True:
            if pages >= _MAX_JSM_PAGES:
                raise RuntimeError("JSM service desk pagination exceeded page cap")
            page = await self._request_jsm_json(
                path="/rest/servicedeskapi/servicedesk",
                params={"start": start, "limit": self.per_page},
            )
            values = page.get("values")
            if not isinstance(values, list):
                raise RuntimeError("JSM service desk response values must be a list")
            is_last_page = page.get("isLastPage")
            if not isinstance(is_last_page, bool):
                raise RuntimeError(
                    "JSM service desk response isLastPage must be a boolean"
                )
            rows = values
            pages += 1
            rows_seen += len(rows)
            if rows_seen > _MAX_JSM_ROWS:
                raise RuntimeError("JSM service desk pagination exceeded row cap")
            project_keys: list[str] = []
            for row in rows:
                if isinstance(row, dict):
                    project_key = row.get("projectKey")
                    if isinstance(project_key, str):
                        project_keys.append(project_key)
            for project_key in project_keys:
                yield project_key
            if is_last_page:
                return
            if not rows:
                raise RuntimeError(
                    "JSM service desk pagination ended without terminal marker"
                )
            next_start = start + len(rows)
            if next_start <= start:
                raise RuntimeError("JSM service desk pagination did not advance")
            start = next_start

    async def iter_jsm_incident_issues(
        self,
        *,
        project_keys: tuple[str, ...],
        window_start: datetime,
        window_end: datetime,
    ) -> AsyncIterator[dict[str, object]]:
        """Search JSM incident issues through enhanced JQL token pagination."""
        if not project_keys:
            return
        await self._ensure_jsm_cloud_identity()
        if window_start >= window_end:
            raise RuntimeError("JSM enhanced JQL requires a non-empty updated window")
        jql = (
            f'project in ({", ".join(project_keys)}) AND "Ticket category" = Incidents '
            f'AND updated >= "{window_start.isoformat()}" '
            f'AND updated < "{window_end.isoformat()}" '
            "ORDER BY updated ASC, key ASC"
        )
        token: str | None = None
        seen_tokens: set[str] = set()
        pages = 0
        rows_seen = 0
        while True:
            if pages >= _MAX_JSM_PAGES:
                raise RuntimeError("JSM enhanced JQL pagination exceeded page cap")
            body: dict[str, JsonValue] = {
                "jql": jql,
                "maxResults": self.per_page,
                "fields": list(_JSM_INCIDENT_FIELDS),
            }
            if token is not None:
                body["nextPageToken"] = token
            page = await self._request_jsm_json(
                method="POST", path="/rest/api/3/search/jql", json=body
            )
            issues = page.get("issues")
            if not isinstance(issues, list):
                raise RuntimeError("JSM enhanced JQL response issues must be a list")
            is_last = page.get("isLast")
            if not isinstance(is_last, bool):
                raise RuntimeError("JSM enhanced JQL response isLast must be a boolean")
            rows = issues
            pages += 1
            rows_seen += len(rows)
            if rows_seen > _MAX_JSM_ROWS:
                raise RuntimeError("JSM enhanced JQL pagination exceeded row cap")
            for row in rows:
                if not isinstance(row, dict):
                    raise RuntimeError("JSM enhanced JQL issue rows must be objects")
                yield dict(row)
            next_token = page.get("nextPageToken")
            if is_last:
                return
            if (
                not isinstance(next_token, str)
                or not next_token
                or next_token in seen_tokens
            ):
                raise RuntimeError("JSM enhanced JQL nextPageToken did not advance")
            seen_tokens.add(next_token)
            token = next_token

    async def admit_jsm_incident(self, *, issue_id: str) -> bool:
        """Public admission boundary: native success alone accepts a JQL candidate."""
        cloud_id = await self._ensure_jsm_cloud_identity()
        return await run_sync(
            partial(
                self._admit_jsm_incident_sync,
                cloud_id=cloud_id,
                issue_id=issue_id,
            )
        )

    def _admit_jsm_incident_sync(self, *, cloud_id: str, issue_id: str) -> bool:
        """Use a fresh fixed-host request so the Jira-site session cannot cross hosts."""
        if not issue_id.isdecimal():
            raise RuntimeError(
                "JSM native incident admission requires a numeric issue ID"
            )
        path = (
            "/jsm/incidents/cloudId/"
            f"{quote(cloud_id, safe='')}/v1/incident/{quote(issue_id, safe='')}"
        )
        url = f"{_JSM_INCIDENTS_API_ORIGIN}{path}"
        attempts = self.max_retries_429 + 1
        for attempt in range(attempts):
            self.gate.wait_sync()
            response = requests.get(
                url,
                auth=(self.auth.email, self.auth.api_token),
                headers={"Accept": "application/json"},
                timeout=self.timeout_seconds,
                allow_redirects=False,
            )
            self._record_rest_usage(
                f"jira_jsm_incident_admission:GET {url}",
                headers=response.headers,
                status=response.status_code,
            )
            if 300 <= response.status_code < 400:
                raise RuntimeError("JSM incident reads do not follow redirects")
            if response.status_code == 404:
                self.gate.reset()
                return False
            if response.status_code == 429:
                retry_after = parse_retry_after_header(response.headers)
                applied = penalize_from_response(self.gate, response)
                logger.info(
                    "Jira rate limited; backoff %.1fs (HTTP 429, attempt %d/%d)",
                    applied,
                    attempt + 1,
                    attempts,
                )
                if attempt + 1 < attempts:
                    continue
                raise RateLimitException(
                    f"Jira rate limited: giving up after {attempts} attempts (HTTP 429)",
                    retry_after_seconds=retry_after,
                    signal=RateLimitSignal(
                        provider="jira",
                        host="api.atlassian.com",
                        dimension=BudgetDimension.REST_CORE,
                        retry_after_seconds=retry_after,
                        reset_at=RateLimitSignal.reset_at_from_iso8601(
                            response.headers.get("X-RateLimit-Reset")
                        ),
                        reason="primary",
                        request_id=response.headers.get("X-AREQUESTID"),
                    ),
                )
            if response.status_code != 200:
                response.raise_for_status()
                raise RuntimeError("JSM native incident admission requires HTTP 200")
            self.gate.reset()
            response_body = response.json()
            if not isinstance(response_body, dict):
                raise RuntimeError(
                    "JSM native incident response JSON must be an object"
                )
            parse_jsm_native_incident(response_body)
            return True
        raise RuntimeError(
            "JSM native incident admission retry loop exited without a result"
        )

    async def _request_jsm_json(
        self,
        *,
        path: str,
        params: Mapping[str, str | int] | None = None,
        method: Literal["GET", "POST"] = "GET",
        json: dict[str, JsonValue] | None = None,
    ) -> dict[str, JsonValue]:
        """Run JSM's synchronous HTTP and rate-gate work off the event loop."""
        return await run_sync(
            partial(
                self._request_json,
                path=path,
                params=params,
                method=method,
                json=json,
                allow_redirects=False,
            )
        )

    def discover_jsm_cloud_id(self, *, expected_cloud_id: str | None = None) -> str:
        """Discover and optionally verify the Cloud ID from the trusted tenant endpoint."""
        validate_jsm_cloud_origin(self.auth.base_url)
        tenant_info = self._request_json(
            path="/_edge/tenant_info", allow_redirects=False
        )
        cloud_id = tenant_info.get("cloudId")
        if not isinstance(cloud_id, str) or not cloud_id:
            raise RuntimeError("JSM tenant info response must contain cloudId")
        if expected_cloud_id is not None and cloud_id != expected_cloud_id:
            raise RuntimeError(
                "JSM discovered cloud ID does not match configured cloud ID"
            )
        return cloud_id

    async def _ensure_jsm_cloud_identity(self) -> str:
        """Discover the JSM tenant in a worker thread before incident-only reads."""
        if self._jsm_cloud_id is not None:
            return self._jsm_cloud_id
        validate_jsm_cloud_origin(self.auth.base_url)
        tenant_info = await self._request_jsm_json(path="/_edge/tenant_info")
        cloud_id = tenant_info.get("cloudId")
        if not isinstance(cloud_id, str) or not cloud_id:
            raise RuntimeError("JSM tenant info response must contain cloudId")
        expected_cloud_id = self.auth.cloud_id
        if expected_cloud_id is not None and cloud_id != expected_cloud_id:
            raise RuntimeError(
                "JSM discovered cloud ID does not match configured cloud ID"
            )
        self._jsm_cloud_id = cloud_id
        return cloud_id


def discover_jsm_cloud_id(
    client: JiraClient, *, expected_cloud_id: str | None = None
) -> str:
    """Discover the trusted Jira Cloud ID for a configured JSM client."""
    return client.discover_jsm_cloud_id(expected_cloud_id=expected_cloud_id)


def build_jira_jql(
    *,
    project_key: str | None = None,
    updated_since: str | None = None,
    active_until: str | None = None,
) -> str:
    """
    Basic JQL builder used by the daily metrics job.

    - project_key: e.g. "ABC"
    - updated_since: ISO date string accepted by Jira JQL, e.g. "2025-01-01"
    """
    clauses = []
    if updated_since and active_until:
        # Also include still-open items that may not have been updated recently, but existed within the window.
        # Prefer statusCategory over resolution: resolution can remain set on reopened issues, while statusCategory
        # is Jira's normalized open/done bucketing.
        clauses.append(
            f"(updated >= '{updated_since}' OR (statusCategory != Done AND created <= '{active_until}'))"
        )
    elif updated_since:
        clauses.append(f"updated >= '{updated_since}'")
    elif active_until:
        clauses.append(f"created <= '{active_until}'")

    if project_key:
        clauses.insert(0, f"project = '{project_key}'")

    where = " AND ".join(clauses)
    if where:
        return f"{where} ORDER BY updated DESC"
    return "ORDER BY updated DESC"
