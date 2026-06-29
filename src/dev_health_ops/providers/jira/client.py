from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

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
from dev_health_ops.providers.utils import EnvSpec, read_env_spec

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
_MAX_USAGE_OBSERVATION_KEYS = 50


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


@dataclass(frozen=True)
class JiraAuth:
    base_url: str
    email: str
    api_token: str


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
        self._usage_observations: dict[tuple[str, str], dict[str, Any]] = {}
        self._usage_observation_overflow = 0

        self.session = requests.Session()
        self.session.auth = (auth.email, auth.api_token)
        self.session.headers.update({"Accept": "application/json"})

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

    def _request_json(self, *, path: str, params: dict[str, Any]) -> dict[str, Any]:
        import requests

        url = self._url(path)
        attempts = self.max_retries_429 + 1
        for attempt in range(attempts):
            self.gate.wait_sync()
            try:
                resp = self.session.get(
                    url, params=params, timeout=self.timeout_seconds
                )
                self._record_rest_usage(
                    f"GET {path}", headers=resp.headers, status=resp.status_code
                )
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
                    )
                resp.raise_for_status()
                self.gate.reset()
                data = resp.json()
                return data if isinstance(data, dict) else {}
            except requests.HTTPError as exc:
                try:
                    body = exc.response.text if exc.response is not None else ""
                except Exception:
                    body = ""
                logger.debug(
                    "Jira request failed: %s %s params=%s body=%s",
                    "GET",
                    url,
                    params,
                    body,
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
        if not headers and not rate_limit and status is None:
            return
        key = (transport, operation)
        observation = self._usage_observations.get(key)
        if observation is None:
            if len(self._usage_observations) >= _MAX_USAGE_OBSERVATION_KEYS:
                self._usage_observation_overflow += 1
                return
            observation = {
                "transport": transport,
                "operation": operation,
                "request_count": 0,
            }
            self._usage_observations[key] = observation
        observation["request_count"] = int(observation["request_count"]) + 1
        if status is not None:
            observation["latest_status"] = status
        if headers:
            observation["latest_headers"] = dict(headers)
        if rate_limit:
            observation["rate_limit"] = dict(rate_limit)

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
        observations = [dict(value) for value in self._usage_observations.values()]
        if self._usage_observation_overflow:
            observations.append(
                {
                    "transport": "summary",
                    "operation": "overflow",
                    "dropped_operation_count": self._usage_observation_overflow,
                }
            )
        self._usage_observations.clear()
        self._usage_observation_overflow = 0
        return observations

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
            boards = list((page or {}).get("values") or [])
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
            sprints = list((page or {}).get("values") or [])
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
        projects = []
        start_at = 0
        max_results = 50

        while True:
            params = {
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
            except Exception:
                # Fallback to non-paginated (or differently paginated) /project endpoint
                # which usually returns all projects if the list is small, or
                # strictly follows deprecated behavior.
                # Ideally, we stick to /search. If it fails, we might just re-raise.
                logger.warning(
                    "Jira project/search failed, trying /project (may be unpaginated)"
                )
                return self._request_json(path="/rest/api/3/project", params={})  # type: ignore

            if not page:
                break

            projects.extend(page)
            if data.get("isLast"):
                break

            start_at += len(page)
            # Safety break for massive instances if isLast isn't reliable
            if len(page) < max_results:
                break

        return projects


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
