from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import httpx

from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
    create_rate_limit_gate,
)
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.providers._ratelimit import gate_call
from dev_health_ops.providers.usage import UsageRecorder
from dev_health_ops.providers.utils import EnvSpec, read_env_spec
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal

logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"

DEFAULT_MAX_ATTEMPTS = 5

_GRAPHQL_OPERATION_NAME_RE = re.compile(r"\b(?:query|mutation)\s+(\w+)")

# Linear returns per-request budget headers on every GraphQL POST; capturing
# them lets recorded actuals join against the GraphQL-cost budget estimate.
_LINEAR_RATE_LIMIT_HEADERS = (
    ("x-ratelimit-requests-limit", "limit"),
    ("x-ratelimit-requests-remaining", "remaining"),
    ("x-ratelimit-requests-reset", "reset"),
    ("retry-after", "retry_after"),
)


def _graphql_operation_name(query: str) -> str:
    """Extract the named GraphQL operation (e.g. ``Issues``) for route-family
    resolution; falls back to ``graphql`` for anonymous queries."""

    match = _GRAPHQL_OPERATION_NAME_RE.search(query)
    return match.group(1) if match else "graphql"


class LinearGraphQLError(RuntimeError):
    """Linear returned GraphQL-level errors for a request.

    Subclasses ``RuntimeError`` for backwards compatibility with callers
    that previously caught the generic error raised here.
    """


class LinearComplexityLimitError(LinearGraphQLError):
    """Linear rejected the query for exceeding its GraphQL complexity limit.

    Not retryable: the query itself must be restructured (e.g. smaller
    nested page sizes). See Linear's 10,000-complexity budget.

    Carries an optional provider-neutral :class:`RateLimitSignal` (dimension
    ``graphql_cost``) so complexity rejections surface in the same observability
    stream as timed rate limits -- but, crucially, it stays a
    :class:`LinearGraphQLError` (not a ``RateLimitException``) so the worker
    deferral branch never re-drives it as retryable work.
    """

    def __init__(self, *args: object, signal: RateLimitSignal | None = None) -> None:
        super().__init__(*args)
        self.signal = signal


class LinearRateLimitError(RateLimitException):
    """Linear kept returning HTTP 429 after exhausting all retry attempts."""


class _LinearHTTPRateLimit(RuntimeError):
    def __init__(self, retry_after_seconds: float | None) -> None:
        super().__init__("Linear HTTP 429")
        self.retry_after_seconds = retry_after_seconds


ISSUES_QUERY = """
query Issues($first: Int!, $after: String, $filter: IssueFilter) {
  issues(first: $first, after: $after, filter: $filter, orderBy: updatedAt) {
    nodes {
      id
      identifier
      number
      title
      description
      priority
      estimate
      createdAt
      updatedAt
      startedAt
      completedAt
      canceledAt
      archivedAt
      dueDate
      url
      state {
        id
        name
        type
      }
      assignee {
        id
        name
        email
      }
      creator {
        id
        name
        email
      }
      labels {
        nodes {
          id
          name
        }
      }
      parent {
        id
        identifier
      }
      project {
        id
        name
      }
      cycle {
        id
        number
        name
        startsAt
        endsAt
      }
      team {
        id
        key
        name
      }
      history(first: 50) {
        nodes {
          id
          createdAt
          fromState {
            id
            name
            type
          }
          toState {
            id
            name
            type
          }
          actor {
            id
            name
            email
          }
        }
      }
      comments(first: 50) {
        nodes {
          id
          body
          createdAt
          updatedAt
          user {
            id
            name
            email
          }
        }
      }
      attachments(first: 50) {
        nodes {
          id
          url
          sourceType
        }
        pageInfo {
          hasNextPage
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

TEAMS_QUERY = """
query Teams($first: Int!, $after: String) {
  teams(first: $first, after: $after) {
    nodes {
      id
      key
      name
      description
      createdAt
      updatedAt
      timezone
      members(first: 10) {
        nodes {
          id
          name
          email
          active
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

TEAM_BY_KEY_QUERY = """
query TeamByKey($first: Int!, $filter: TeamFilter) {
  teams(first: $first, filter: $filter) {
    nodes {
      id
      key
      name
      description
      createdAt
      updatedAt
      timezone
    }
  }
}
"""

TEAM_MEMBERS_QUERY = """
query TeamMembers($teamId: String!, $first: Int!, $after: String) {
  team(id: $teamId) {
    members(first: $first, after: $after) {
      nodes {
        id
        name
        email
        active
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""

CYCLES_QUERY = """
query Cycles($first: Int!, $after: String, $filter: CycleFilter) {
  cycles(first: $first, after: $after, filter: $filter) {
    nodes {
      id
      number
      name
      description
      startsAt
      endsAt
      completedAt
      progress
      team {
        id
        key
        name
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

PROJECTS_QUERY = """
query Projects($first: Int!, $after: String) {
  projects(first: $first, after: $after) {
    nodes {
      id
      name
      description
      state
      progress
      startDate
      targetDate
      createdAt
      updatedAt
      url
      lead {
        id
        name
        email
      }
      teams {
        nodes {
          id
          key
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

COMMENTS_QUERY = """
query Comments($issueId: String!, $first: Int!, $after: String) {
  issue(id: $issueId) {
    comments(first: $first, after: $after) {
      nodes {
        id
        body
        createdAt
        updatedAt
        user {
          id
          name
          email
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""

ATTACHMENTS_QUERY = """
query Attachments($issueId: String!, $first: Int!, $after: String) {
  issue(id: $issueId) {
    attachments(first: $first, after: $after) {
      nodes {
        id
        url
        sourceType
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""

ISSUE_HISTORY_QUERY = """
query IssueHistory($issueId: String!) {
  issue(id: $issueId) {
    history(first: 100) {
      nodes {
        id
        createdAt
        fromState {
          id
          name
          type
        }
        toState {
          id
          name
          type
        }
        actor {
          id
          name
          email
        }
      }
    }
  }
}
"""

WORKFLOW_STATES_QUERY = """
query WorkflowStates($first: Int!, $after: String) {
  workflowStates(first: $first, after: $after) {
    nodes {
      id
      name
      type
      position
      team {
        id
        key
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


@dataclass(frozen=True)
class LinearAuth:
    api_key: str


@dataclass
class RateLimitInfo:
    limit: int
    remaining: int
    reset_ms: int


class LinearClient:
    def __init__(
        self,
        *,
        auth: LinearAuth,
        per_page: int = 50,
        gate: RateLimitGate | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        org_id: str | None = None,
    ) -> None:
        self.auth = auth
        self.per_page = max(1, min(100, int(per_page)))
        host = urlparse(LINEAR_API_URL).hostname or "api.linear.app"
        self.gate = gate or create_rate_limit_gate(
            "linear",
            org_id=org_id,
            host=host,
            config=RateLimitConfig(initial_backoff_seconds=1.0),
        )
        self.max_attempts = max(1, int(max_attempts))
        self._rate_limit: RateLimitInfo | None = None

        from dev_health_ops.providers.linear.budget import LINEAR_USAGE_RESOLVER

        self._usage = UsageRecorder(resolver=LINEAR_USAGE_RESOLVER)
        self._client = httpx.Client(
            headers={
                "Content-Type": "application/json",
                "Authorization": auth.api_key,
            },
            timeout=30.0,
        )

    @classmethod
    def from_env(cls, *, org_id: str | None = None) -> LinearClient:
        env = read_env_spec(
            EnvSpec(
                required={"api_key": "LINEAR_API_KEY"},
                missing_error="Linear API key required (set LINEAR_API_KEY)",
            )
        )
        return cls(auth=LinearAuth(api_key=str(env["api_key"])), org_id=org_id)

    def _execute(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        last_delay = 0.0
        last_server_retry_after: float | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                with gate_call(self.gate):
                    self._wait_for_rate_limit()
                    response = self._client.post(LINEAR_API_URL, json=payload)
                    self._update_rate_limit(response)
                    # Count every POST (including the ones that 429) so recorded
                    # actuals reflect real GraphQL request volume.
                    self._record_graphql_usage(query, response)
                    if response.status_code == 429:
                        raise _LinearHTTPRateLimit(self._retry_after_seconds(response))
            except _LinearHTTPRateLimit as exc:
                if exc.retry_after_seconds is not None:
                    last_server_retry_after = exc.retry_after_seconds
                last_delay = self._last_applied_delay(exc.retry_after_seconds)
                logger.warning(
                    "Linear rate limit hit (attempt %d/%d), backing off %.1fs",
                    attempt,
                    self.max_attempts,
                    last_delay,
                )
                continue

            body = self._parse_body(response)

            # GraphQL-level errors (including complexity-limit rejections,
            # which Linear returns as HTTP 400) are never retried.
            errors = body.get("errors") if isinstance(body, dict) else None
            if errors:
                self._raise_graphql_errors(errors)

            response.raise_for_status()
            data = body if isinstance(body, dict) else {}
            return data.get("data") or {}

        retry_after = (
            last_server_retry_after
            if last_server_retry_after is not None
            else last_delay
        )
        raise LinearRateLimitError(
            f"Linear API rate limited: giving up after {self.max_attempts} "
            f"attempts (last backoff {last_delay:.1f}s)",
            retry_after_seconds=retry_after,
            signal=RateLimitSignal(
                provider="linear",
                host=urlparse(LINEAR_API_URL).netloc or None,
                dimension=BudgetDimension.GRAPHQL_COST,
                retry_after_seconds=retry_after,
                # Linear reports its reset window as epoch MILLISECONDS.
                reset_at=RateLimitSignal.reset_at_from_epoch_millis(
                    self._rate_limit.reset_ms if self._rate_limit else None
                ),
                reason="primary",
            ),
        )

    def _last_applied_delay(self, retry_after_seconds: float | None) -> float:
        if retry_after_seconds is not None:
            return max(
                0.0,
                min(float(retry_after_seconds), self.gate._config.max_backoff_seconds),
            )
        factor = self.gate._config.backoff_factor or 1.0
        if factor <= 0:
            return self.gate._config.initial_backoff_seconds
        return min(
            self.gate._current_backoff / factor,
            self.gate._config.max_backoff_seconds,
        )

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        try:
            raw = response.headers.get("Retry-After")
            return float(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_body(response: httpx.Response) -> Any:
        try:
            return response.json()
        except ValueError:
            return None

    @staticmethod
    def _raise_graphql_errors(errors: list[dict[str, Any]]) -> None:
        error_msg = "; ".join(e.get("message", str(e)) for e in errors)
        if any(LinearClient._is_complexity_error(e) for e in errors):
            raise LinearComplexityLimitError(
                "Linear GraphQL complexity limit exceeded "
                f"(query must be restructured, not retried): {error_msg}",
                signal=RateLimitSignal(
                    provider="linear",
                    host=urlparse(LINEAR_API_URL).netloc or None,
                    dimension=BudgetDimension.GRAPHQL_COST,
                    reason="complexity",
                ),
            )
        raise LinearGraphQLError(f"Linear GraphQL error: {error_msg}")

    @staticmethod
    def _is_complexity_error(error: dict[str, Any]) -> bool:
        message = str(error.get("message", "")).lower()
        if "complexity" in message or "too complex" in message:
            return True
        extensions = error.get("extensions") or {}
        code = str(extensions.get("code", "")).upper()
        return "COMPLEXITY" in code

    def _wait_for_rate_limit(self) -> None:
        if self._rate_limit is None:
            return

        if self._rate_limit.remaining <= 5:
            now_ms = int(time.time() * 1000)
            wait_ms = self._rate_limit.reset_ms - now_ms
            if wait_ms > 0:
                wait_s = wait_ms / 1000 + 1
                logger.info("Linear rate limit low, waiting %.1fs", wait_s)
                time.sleep(wait_s)

    def _update_rate_limit(self, response: httpx.Response) -> None:
        headers = response.headers
        try:
            limit = int(headers.get("X-RateLimit-Requests-Limit", 1500))
            remaining = int(headers.get("X-RateLimit-Requests-Remaining", 1500))
            reset_ms = int(headers.get("X-RateLimit-Requests-Reset", 0))
            self._rate_limit = RateLimitInfo(
                limit=limit, remaining=remaining, reset_ms=reset_ms
            )
        except (ValueError, TypeError):
            logger.warning(
                "Failed to parse Linear rate limit headers; continuing without rate limit info. "
                "Headers: %s",
                dict(headers),
            )

    def _record_graphql_usage(self, query: str, response: httpx.Response) -> None:
        rate_limit: dict[str, Any] = {}
        for source, target in _LINEAR_RATE_LIMIT_HEADERS:
            value = response.headers.get(source)
            if value is not None:
                rate_limit[target] = str(value)
        self._usage.record(
            transport="graphql",
            operation=_graphql_operation_name(query),
            headers={},
            rate_limit=rate_limit,
            status=response.status_code,
        )

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return self._usage.drain()

    def iter_issues(
        self,
        *,
        team_keys: list[str] | None = None,
        updated_after: datetime | None = None,
        updated_before: datetime | None = None,
        include_archived: bool = False,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        count = 0

        for nodes in self.iter_issues_pages(
            team_keys=team_keys,
            updated_after=updated_after,
            updated_before=updated_before,
            include_archived=include_archived,
        ):
            for node in nodes:
                yield node
                count += 1
                if limit is not None and count >= limit:
                    return

    def iter_issues_pages(
        self,
        *,
        team_keys: list[str] | None = None,
        updated_after: datetime | None = None,
        updated_before: datetime | None = None,
        include_archived: bool = False,
    ) -> Iterable[list[dict[str, Any]]]:
        cursor: str | None = None

        filter_obj: dict[str, Any] = {}
        if team_keys:
            filter_obj["team"] = {"key": {"in": team_keys}}
        updated_at_filter: dict[str, Any] = {}
        if updated_after:
            updated_at_filter["gte"] = updated_after.isoformat()
        if updated_before:
            updated_at_filter["lte"] = updated_before.isoformat()
        if updated_at_filter:
            filter_obj["updatedAt"] = updated_at_filter
        if not include_archived:
            filter_obj["archivedAt"] = {"null": True}

        while True:
            variables: dict[str, Any] = {
                "first": self.per_page,
                "after": cursor,
            }
            if filter_obj:
                variables["filter"] = filter_obj

            data = self._execute(ISSUES_QUERY, variables)
            issues_data = data.get("issues", {})
            nodes = issues_data.get("nodes", [])
            if nodes:
                yield nodes

            page_info = issues_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def get_issue_comments(
        self,
        issue_id: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        cursor: str | None = None
        comments: list[dict[str, Any]] = []

        while len(comments) < limit:
            variables = {
                "issueId": issue_id,
                "first": min(50, limit - len(comments)),
                "after": cursor,
            }
            data = self._execute(COMMENTS_QUERY, variables)
            issue_data = data.get("issue", {})
            comments_data = issue_data.get("comments", {})
            nodes = comments_data.get("nodes", [])
            comments.extend(nodes)

            page_info = comments_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        return comments[:limit]

    def get_issue_attachments(
        self,
        issue_id: str,
        *,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Fetch ALL of an issue's attachments, paginating the connection.

        Used only when the bulk ``ISSUES_QUERY`` page is truncated, so a linked
        PR/MR attachment past the first page is still captured for team
        inheritance instead of being silently dropped.
        """
        cursor: str | None = None
        attachments: list[dict[str, Any]] = []
        while len(attachments) < limit:
            variables = {
                "issueId": issue_id,
                "first": min(100, limit - len(attachments)),
                "after": cursor,
            }
            data = self._execute(ATTACHMENTS_QUERY, variables)
            conn = (data.get("issue") or {}).get("attachments") or {}
            attachments.extend(conn.get("nodes", []))
            page_info = conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break
        return attachments[:limit]

    def get_issue_history(self, issue_id: str) -> list[dict[str, Any]]:
        data = self._execute(ISSUE_HISTORY_QUERY, {"issueId": issue_id})
        issue_data = data.get("issue", {})
        history_data = issue_data.get("history", {})
        return history_data.get("nodes", [])

    def iter_teams(self) -> Iterable[dict[str, Any]]:
        cursor: str | None = None

        while True:
            variables = {"first": self.per_page, "after": cursor}
            data = self._execute(TEAMS_QUERY, variables)
            teams_data = data.get("teams", {})
            nodes = teams_data.get("nodes", [])

            yield from nodes

            page_info = teams_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def get_team_by_key(self, team_key: str) -> dict[str, Any] | None:
        variables = {
            "first": 2,
            "filter": {"key": {"eq": team_key}},
        }
        data = self._execute(TEAM_BY_KEY_QUERY, variables)
        nodes = (data.get("teams") or {}).get("nodes") or []
        for node in nodes:
            if node.get("key") == team_key or node.get("name") == team_key:
                return node
        return None

    def get_team_members(
        self,
        team_id: str,
    ) -> list[dict[str, Any]]:
        """Fetch all active members for a team, with pagination for teams >100 members."""
        cursor: str | None = None
        members: list[dict[str, Any]] = []

        while True:
            variables: dict[str, Any] = {
                "teamId": team_id,
                "first": self.per_page,
                "after": cursor,
            }
            data = self._execute(TEAM_MEMBERS_QUERY, variables)
            team_data = data.get("team", {})
            members_data = team_data.get("members", {})
            nodes = members_data.get("nodes", [])
            # Filter out inactive users
            members.extend([m for m in nodes if m.get("active", True)])

            page_info = members_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        return members

    def iter_cycles(
        self,
        *,
        team_id: str | None = None,
    ) -> Iterable[dict[str, Any]]:
        cursor: str | None = None

        filter_obj: dict[str, Any] = {}
        if team_id:
            filter_obj["team"] = {"id": {"eq": team_id}}

        while True:
            variables: dict[str, Any] = {"first": self.per_page, "after": cursor}
            if filter_obj:
                variables["filter"] = filter_obj

            data = self._execute(CYCLES_QUERY, variables)
            cycles_data = data.get("cycles", {})
            nodes = cycles_data.get("nodes", [])

            yield from nodes

            page_info = cycles_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def iter_projects(self) -> Iterable[dict[str, Any]]:
        cursor: str | None = None

        while True:
            variables = {"first": self.per_page, "after": cursor}
            data = self._execute(PROJECTS_QUERY, variables)
            projects_data = data.get("projects", {})
            nodes = projects_data.get("nodes", [])

            yield from nodes

            page_info = projects_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def iter_workflow_states(self) -> Iterable[dict[str, Any]]:
        cursor: str | None = None

        while True:
            variables = {"first": self.per_page, "after": cursor}
            data = self._execute(WORKFLOW_STATES_QUERY, variables)
            states_data = data.get("workflowStates", {})
            nodes = states_data.get("nodes", [])

            yield from nodes

            page_info = states_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> LinearClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
