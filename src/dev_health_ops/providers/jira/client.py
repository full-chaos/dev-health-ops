from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional

from dev_health_ops.connectors.utils import retry_with_backoff
from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)

logger = logging.getLogger(__name__)


def _require_jira() -> Any:
    try:
        from jira import JIRA  # type: ignore

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
        gate: Optional[RateLimitGate] = None,
    ) -> None:
        import requests

        self.auth = auth
        self.timeout_seconds = int(timeout_seconds)
        self.per_page = max(1, min(100, int(per_page)))
        self.gate = gate or RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))

        self.session = requests.Session()
        self.session.auth = (auth.email, auth.api_token)
        self.session.headers.update({"Accept": "application/json"})

    @classmethod
    def from_env(cls) -> "JiraClient":
        base_url = os.getenv("JIRA_BASE_URL") or ""
        email = os.getenv("JIRA_EMAIL") or ""
        api_token = os.getenv("JIRA_API_TOKEN") or ""
        if not base_url or not email or not api_token:
            raise ValueError(
                "Jira env vars required: JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN"
            )
        return cls(
            auth=JiraAuth(
                base_url=_normalize_jira_base_url(base_url),
                email=email,
                api_token=api_token,
            )
        )

    def close(self) -> None:
        try:
            self.session.close()
        except Exception as exc:
            logger.debug("Error while closing JiraClient session", exc_info=exc)

    def _url(self, path: str) -> str:
        return f"{self.auth.base_url}{path}"

    def _request_json(self, *, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        import requests

        url = self._url(path)
        self.gate.wait_sync()
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout_seconds)
            if resp.status_code == 429:
                retry_after = None
                try:
                    retry_after = float(resp.headers.get("Retry-After") or "")
                except Exception:
                    retry_after = None
                applied = self.gate.penalize(retry_after)
                logger.info("Jira rate limited; backoff %.1fs (HTTP 429)", applied)
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
                "Jira request failed: %s %s params=%s body=%s", "GET", url, params, body
            )
            raise

    @retry_with_backoff(max_retries=5, initial_delay=1.0, max_delay=60.0)
    def search_issues_page(
        self,
        *,
        jql: str,
        start_at: int,
        max_results: int,
        fields: Optional[Iterable[str]] = None,
        expand: Optional[str] = None,
        next_page_token: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {
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
        fields: Optional[Iterable[str]] = None,
        expand_changelog: bool = True,
        limit: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterate issues matching a JQL query with pagination.

        NOTE: Jira may truncate changelogs on large issues. For full history,
        callers may need to fetch per-issue changelogs separately.
        """
        start_at = 0
        fetched = 0
        expand = "changelog" if expand_changelog else None
        next_page_token: Optional[str] = None

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

    @retry_with_backoff(max_retries=5, initial_delay=1.0, max_delay=60.0)
    def fetch_issue_comments_page(
        self,
        *,
        issue_id_or_key: str,
        start_at: int,
        max_results: int,
    ) -> Any:
        params: Dict[str, Any] = {
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
        limit: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
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

    @retry_with_backoff(max_retries=5, initial_delay=1.0, max_delay=60.0)
    def get_sprint(self, *, sprint_id: str) -> Dict[str, Any]:
        return self._request_json(
            path=f"/rest/agile/1.0/sprint/{sprint_id}",
            params={},
        )

    def get_all_projects(self) -> List[Dict[str, Any]]:
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
    project_key: Optional[str] = None,
    updated_since: Optional[str] = None,
    active_until: Optional[str] = None,
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
