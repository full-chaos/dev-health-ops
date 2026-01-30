from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import httpx

logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"

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
    }
    pageInfo {
      hasNextPage
      endCursor
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
    ) -> None:
        self.auth = auth
        self.per_page = max(1, min(100, int(per_page)))
        self._rate_limit: Optional[RateLimitInfo] = None
        self._client = httpx.Client(
            headers={
                "Content-Type": "application/json",
                "Authorization": auth.api_key,
            },
            timeout=30.0,
        )

    @classmethod
    def from_env(cls) -> "LinearClient":
        api_key = os.getenv("LINEAR_API_KEY") or ""
        if not api_key:
            raise ValueError("Linear API key required (set LINEAR_API_KEY)")
        return cls(auth=LinearAuth(api_key=api_key))

    def _execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._wait_for_rate_limit()

        payload: Dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        response = self._client.post(LINEAR_API_URL, json=payload)
        self._update_rate_limit(response)

        if response.status_code == 429:
            retry_after = self._get_retry_after(response)
            logger.warning("Linear rate limit hit, waiting %ds", retry_after)
            time.sleep(retry_after)
            return self._execute(query, variables)

        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            errors = data["errors"]
            error_msg = "; ".join(e.get("message", str(e)) for e in errors)
            raise RuntimeError(f"Linear GraphQL error: {error_msg}")

        return data.get("data", {})

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

    def _get_retry_after(self, response: httpx.Response) -> int:
        try:
            reset_ms = int(response.headers.get("X-RateLimit-Requests-Reset", 0))
            now_ms = int(time.time() * 1000)
            wait_s = max(1, (reset_ms - now_ms) // 1000 + 1)
            return min(wait_s, 3600)
        except (ValueError, TypeError):
            return 60

    def iter_issues(
        self,
        *,
        team_keys: Optional[List[str]] = None,
        updated_after: Optional[datetime] = None,
        include_archived: bool = False,
        limit: Optional[int] = None,
    ) -> Iterable[Dict[str, Any]]:
        cursor: Optional[str] = None
        count = 0

        filter_obj: Dict[str, Any] = {}
        if team_keys:
            filter_obj["team"] = {"key": {"in": team_keys}}
        if updated_after:
            filter_obj["updatedAt"] = {"gte": updated_after.isoformat()}
        if not include_archived:
            filter_obj["archivedAt"] = {"null": True}

        while True:
            variables: Dict[str, Any] = {
                "first": self.per_page,
                "after": cursor,
            }
            if filter_obj:
                variables["filter"] = filter_obj

            data = self._execute(ISSUES_QUERY, variables)
            issues_data = data.get("issues", {})
            nodes = issues_data.get("nodes", [])

            for node in nodes:
                yield node
                count += 1
                if limit is not None and count >= limit:
                    return

            page_info = issues_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def get_issue_comments(
        self,
        issue_id: str,
        *,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        cursor: Optional[str] = None
        comments: List[Dict[str, Any]] = []

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

    def get_issue_history(self, issue_id: str) -> List[Dict[str, Any]]:
        data = self._execute(ISSUE_HISTORY_QUERY, {"issueId": issue_id})
        issue_data = data.get("issue", {})
        history_data = issue_data.get("history", {})
        return history_data.get("nodes", [])

    def iter_teams(self) -> Iterable[Dict[str, Any]]:
        cursor: Optional[str] = None

        while True:
            variables = {"first": self.per_page, "after": cursor}
            data = self._execute(TEAMS_QUERY, variables)
            teams_data = data.get("teams", {})
            nodes = teams_data.get("nodes", [])

            for node in nodes:
                yield node

            page_info = teams_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def iter_cycles(
        self,
        *,
        team_id: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        cursor: Optional[str] = None

        filter_obj: Dict[str, Any] = {}
        if team_id:
            filter_obj["team"] = {"id": {"eq": team_id}}

        while True:
            variables: Dict[str, Any] = {"first": self.per_page, "after": cursor}
            if filter_obj:
                variables["filter"] = filter_obj

            data = self._execute(CYCLES_QUERY, variables)
            cycles_data = data.get("cycles", {})
            nodes = cycles_data.get("nodes", [])

            for node in nodes:
                yield node

            page_info = cycles_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def iter_projects(self) -> Iterable[Dict[str, Any]]:
        cursor: Optional[str] = None

        while True:
            variables = {"first": self.per_page, "after": cursor}
            data = self._execute(PROJECTS_QUERY, variables)
            projects_data = data.get("projects", {})
            nodes = projects_data.get("nodes", [])

            for node in nodes:
                yield node

            page_info = projects_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def iter_workflow_states(self) -> Iterable[Dict[str, Any]]:
        cursor: Optional[str] = None

        while True:
            variables = {"first": self.per_page, "after": cursor}
            data = self._execute(WORKFLOW_STATES_QUERY, variables)
            states_data = data.get("workflowStates", {})
            nodes = states_data.get("nodes", [])

            for node in nodes:
                yield node

            page_info = states_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "LinearClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
