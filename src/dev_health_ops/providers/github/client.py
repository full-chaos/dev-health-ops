from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from dev_health_ops.connectors.utils.graphql import GitHubGraphQLClient
from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GitHubAuth:
    token: str
    base_url: Optional[str] = None  # GitHub Enterprise REST base URL (optional)


class GitHubWorkClient:
    """
    Work-tracking oriented GitHub client:
    - Issues via PyGithub REST
    - Projects v2 via GraphQL
    """

    def __init__(
        self,
        *,
        auth: GitHubAuth,
        per_page: int = 100,
        gate: Optional[RateLimitGate] = None,
    ) -> None:
        from github import Github  # PyGithub

        self.auth = auth
        self.per_page = max(1, min(100, int(per_page)))
        self.gate = gate or RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))

        if auth.base_url:
            self.github = Github(
                base_url=auth.base_url,
                login_or_token=auth.token,
                per_page=self.per_page,
            )
        else:
            self.github = Github(
                login_or_token=auth.token,
                per_page=self.per_page,
            )

        # GraphQL client (api.github.com only for now).
        self.graphql = GitHubGraphQLClient(auth.token)

    def get_repo(self, *, owner: str, repo: str) -> Any:
        return self.github.get_repo(f"{owner}/{repo}")

    def iter_issues(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Iterable[Any]:
        gh_repo = self.get_repo(owner=owner, repo=repo)
        issues = gh_repo.get_issues(state=state, since=since)
        count = 0
        for issue in issues:
            # Exclude pull requests from issues feed.
            if getattr(issue, "pull_request", None) is not None:
                continue
            yield issue
            count += 1
            if limit is not None and count >= int(limit):
                return

    def iter_issue_events(
        self, issue: Any, *, limit: Optional[int] = None
    ) -> Iterable[Any]:
        """
        Iterate issue events (labeled/unlabeled/closed/reopened/assigned/...) via REST.
        """
        events = issue.get_events()
        count = 0
        for ev in events:
            yield ev
            count += 1
            if limit is not None and count >= int(limit):
                return

    def iter_pull_requests(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        sort: str = "updated",
        direction: str = "desc",
        limit: Optional[int] = None,
    ) -> Iterable[Any]:
        """
        Iterate pull requests in a repository via REST.
        """
        gh_repo = self.get_repo(owner=owner, repo=repo)
        pulls = gh_repo.get_pulls(state=state, sort=sort, direction=direction)
        count = 0
        for pr in pulls:
            yield pr
            count += 1
            if limit is not None and count >= int(limit):
                return

    def iter_issue_comments(
        self, issue: Any, *, limit: Optional[int] = None
    ) -> Iterable[Any]:
        """
        Iterate comments on an issue via REST.
        """
        comments = issue.get_comments()
        count = 0
        for comment in comments:
            yield comment
            count += 1
            if limit is not None and count >= int(limit):
                return

    def iter_pr_comments(
        self, pr: Any, *, limit: Optional[int] = None
    ) -> Iterable[Any]:
        """
        Iterate comments on a pull request (issue comments + review comments).
        """
        # Issue-style comments
        for comment in self.iter_issue_comments(pr, limit=limit):
            yield comment

    def iter_pr_review_comments(
        self, pr: Any, *, limit: Optional[int] = None
    ) -> Iterable[Any]:
        """
        Iterate review comments on a pull request.
        """
        comments = pr.get_review_comments()
        count = 0
        for comment in comments:
            yield comment
            count += 1
            if limit is not None and count >= int(limit):
                return

    def iter_repo_milestones(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        limit: Optional[int] = None,
    ) -> Iterable[Any]:
        """
        Iterate milestones in a repository via REST.
        """
        gh_repo = self.get_repo(owner=owner, repo=repo)
        milestones = gh_repo.get_milestones(state=state)
        count = 0
        for ms in milestones:
            yield ms
            count += 1
            if limit is not None and count >= int(limit):
                return

    def iter_project_v2_items(
        self,
        *,
        org_login: str,
        project_number: int,
        first: int = 50,
        max_items: Optional[int] = None,
    ) -> Iterable[Dict[str, Any]]:
        """
        Iterate GitHub Projects v2 items via GraphQL.

        Returns raw dict nodes (parsed GraphQL response).

        Note: This method automatically paginates through all field changes
        for each item, ensuring complete status transition history is captured.
        """
        query = """
        query($login: String!, $number: Int!, $after: String, $first: Int!) {
          organization(login: $login) {
            projectV2(number: $number) {
              id
              title
              items(first: $first, after: $after) {
                nodes {
                  id
                  createdAt
                  updatedAt
                  content {
                    __typename
                    ... on Issue {
                      id
                      number
                      title
                      url
                      state
                      createdAt
                      updatedAt
                      closedAt
                      repository { nameWithOwner }
                      labels(first: 50) { nodes { name } }
                      assignees(first: 10) { nodes { login email name } }
                      author { login email name }
                    }
                    ... on PullRequest {
                      id
                      number
                      title
                      url
                      state
                      createdAt
                      updatedAt
                      closedAt
                      mergedAt
                      repository { nameWithOwner }
                      labels(first: 50) { nodes { name } }
                      assignees(first: 10) { nodes { login email name } }
                      author { login email name }
                    }
                    ... on DraftIssue {
                      id
                      title
                      createdAt
                      updatedAt
                    }
                  }
                  fieldValues(first: 20) {
                    nodes {
                      __typename
                      ... on ProjectV2ItemFieldSingleSelectValue {
                        name
                        field {
                          ... on ProjectV2SingleSelectField {
                            name
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldTextValue {
                        text
                        field { ... on ProjectV2FieldCommon { name } }
                      }
                      ... on ProjectV2ItemFieldIterationValue {
                        title
                        id
                        field { ... on ProjectV2FieldCommon { name } }
                      }
                      ... on ProjectV2ItemFieldNumberValue {
                        number
                        field { ... on ProjectV2FieldCommon { name } }
                      }
                    }
                  }
                  changes(first: 100, orderBy: {field: CREATED_AT, direction: ASC}) {
                    nodes {
                      field {
                        ... on ProjectV2FieldCommon {
                          name
                        }
                      }
                      previousValue {
                        ... on ProjectV2ItemFieldSingleSelectValue {
                          name
                        }
                      }
                      newValue {
                        ... on ProjectV2ItemFieldSingleSelectValue {
                          name
                        }
                      }
                      createdAt
                      actor {
                        login
                      }
                    }
                    pageInfo { hasNextPage endCursor }
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
          }
        }
        """
        after = None
        fetched = 0
        while True:
            self.gate.wait_sync()
            data = self.graphql.query(
                query,
                variables={
                    "login": org_login,
                    "number": int(project_number),
                    "after": after,
                    "first": int(max(1, min(100, first))),
                },
            )
            self.gate.reset()

            org = (data or {}).get("organization") or {}
            project = org.get("projectV2") or {}
            items = (project.get("items") or {}).get("nodes") or []
            page = (project.get("items") or {}).get("pageInfo") or {}

            for item in items:
                # Paginate through all changes for this item if needed
                changes_dict = item.get("changes") or {}
                changes = changes_dict.get("nodes") or []
                changes_page_info = changes_dict.get("pageInfo") or {}

                # If there are more changes, fetch them
                if changes_page_info.get("hasNextPage"):
                    all_changes = []
                    all_changes.extend(changes)
                    changes_cursor = changes_page_info.get("endCursor")

                    # Fetch remaining changes for this specific item
                    while changes_cursor:
                        self.gate.wait_sync()
                        more_changes = self._fetch_item_changes(
                            item_id=item.get("id"),
                            after=changes_cursor,
                        )
                        self.gate.reset()

                        if not more_changes or not more_changes.get("nodes"):
                            break

                        all_changes.extend(more_changes.get("nodes") or [])
                        changes_page_info = more_changes.get("pageInfo") or {}
                        changes_cursor = changes_page_info.get("endCursor")

                        if not changes_page_info.get("hasNextPage"):
                            break

                    # Update the item with all changes
                    changes_dict["nodes"] = all_changes

                yield item
                fetched += 1
                if max_items is not None and fetched >= int(max_items):
                    return

            if not page.get("hasNextPage"):
                return
            after = page.get("endCursor")

    def _fetch_item_changes(
        self,
        *,
        item_id: str,
        after: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch additional changes for a specific ProjectV2Item.

        Returns the changes dict with nodes and pageInfo, or None if the query
        fails or the item is not found.
        """
        query = """
        query($itemId: ID!, $after: String) {
          node(id: $itemId) {
            ... on ProjectV2Item {
              changes(first: 100, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
                nodes {
                  field {
                    ... on ProjectV2FieldCommon {
                      name
                    }
                  }
                  previousValue {
                    ... on ProjectV2ItemFieldSingleSelectValue {
                      name
                    }
                  }
                  newValue {
                    ... on ProjectV2ItemFieldSingleSelectValue {
                      name
                    }
                  }
                  createdAt
                  actor {
                    login
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
          }
        }
        """

        data = self.graphql.query(
            query,
            variables={
                "itemId": item_id,
                "after": after,
            },
        )

        node = (data or {}).get("node") or {}
        return node.get("changes")
