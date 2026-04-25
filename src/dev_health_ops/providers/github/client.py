from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, TypedDict

from dev_health_ops.connectors.utils.github_app import GitHubAppTokenProvider
from dev_health_ops.connectors.utils.graphql import GitHubGraphQLClient
from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)
from dev_health_ops.credentials.resolver import resolve_credentials_sync
from dev_health_ops.credentials.types import GitHubCredentials
from dev_health_ops.providers._ratelimit import gate_call

logger = logging.getLogger(__name__)


class _GitHubIssueLike(Protocol):
    pull_request: object

    def get_events(self) -> Iterable[object]: ...

    def get_comments(self) -> Iterable[object]: ...


class _GitHubPullRequestLike(_GitHubIssueLike, Protocol):
    def get_review_comments(self) -> Iterable[object]: ...


class _GitHubRepositoryLike(Protocol):
    def get_issues(
        self, *args: object, **kwargs: object
    ) -> Iterable[_GitHubIssueLike]: ...

    def get_pulls(
        self, *args: object, **kwargs: object
    ) -> Iterable[_GitHubPullRequestLike]: ...

    def get_milestones(self, *args: object, **kwargs: object) -> Iterable[object]: ...


class ProjectItemChanges(TypedDict, total=False):
    nodes: list[dict[str, object]]
    pageInfo: dict[str, object]


class ProjectV2ItemNode(TypedDict, total=False):
    id: str
    changes: ProjectItemChanges


@dataclass(frozen=True)
class GitHubAuth:
    token: str | None = None
    app_id: str | None = None
    private_key: str | None = None
    installation_id: str | None = None
    base_url: str | None = None  # GitHub Enterprise REST base URL (optional)

    @classmethod
    def from_credentials(cls, credentials: GitHubCredentials) -> GitHubAuth:
        return cls(
            token=credentials.token,
            app_id=credentials.app_id,
            private_key=credentials.private_key,
            installation_id=credentials.installation_id,
            base_url=credentials.base_url,
        )

    @property
    def is_app_auth(self) -> bool:
        return bool(self.app_id and self.private_key and self.installation_id)


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
        gate: RateLimitGate | None = None,
    ) -> None:
        from github import Github  # PyGithub

        self.auth = auth
        self.per_page = max(1, min(100, int(per_page)))
        self.gate = gate or RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))
        self._app_token_provider: GitHubAppTokenProvider | None = None

        token = auth.token
        if auth.is_app_auth:
            assert auth.app_id is not None
            assert auth.private_key is not None
            assert auth.installation_id is not None
            self._app_token_provider = GitHubAppTokenProvider(
                app_id=auth.app_id,
                private_key=auth.private_key,
                installation_id=auth.installation_id,
            )
            token = self._app_token_provider.get_token()
        if not token:
            raise ValueError("GitHubWorkClient requires token or GitHub App auth")

        if auth.base_url:
            self.github = Github(
                base_url=auth.base_url,
                login_or_token=token,
                per_page=self.per_page,
            )
        else:
            self.github = Github(
                login_or_token=token,
                per_page=self.per_page,
            )

        # GraphQL client (api.github.com only for now).
        token_provider = (
            self._app_token_provider.get_token
            if self._app_token_provider is not None
            else None
        )
        self.graphql = GitHubGraphQLClient(token, token_provider=token_provider)

    @classmethod
    def from_env(cls) -> GitHubWorkClient:
        credentials = resolve_credentials_sync("github", allow_env_fallback=True)
        if not isinstance(credentials, GitHubCredentials):
            raise ValueError("Resolved credentials are not GitHub credentials")
        return cls(auth=GitHubAuth.from_credentials(credentials))

    def get_repo(self, *, owner: str, repo: str) -> Any:
        return self.github.get_repo(f"{owner}/{repo}")

    def _iter_with_limit(
        self,
        source: Iterable[Any],
        *,
        limit: int | None,
        skip: Callable[[Any], bool] | None = None,
    ) -> Iterable[Any]:
        """Yield items from ``source`` respecting ``limit`` and optional skip filter.

        ``skip`` receives each item and returns ``True`` when the item should be
        excluded (used for PR-vs-issue filtering on the issues feed).
        """
        if limit is not None and int(limit) <= 0:
            return
        count = 0
        for item in source:
            if skip is not None and skip(item):
                continue
            yield item
            count += 1
            if limit is not None and count >= int(limit):
                return

    def iter_issues(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        since: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[_GitHubIssueLike]:
        gh_repo = self.get_repo(owner=owner, repo=repo)
        issues = gh_repo.get_issues(state=state, since=since)
        yield from self._iter_with_limit(
            issues,
            limit=limit,
            skip=lambda issue: getattr(issue, "pull_request", None) is not None,
        )

    def iter_issue_events(
        self, issue: _GitHubIssueLike, *, limit: int | None = None
    ) -> Iterable[object]:
        """
        Iterate issue events (labeled/unlabeled/closed/reopened/assigned/...) via REST.
        """
        yield from self._iter_with_limit(issue.get_events(), limit=limit)

    def iter_pull_requests(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        sort: str = "updated",
        direction: str = "desc",
        limit: int | None = None,
    ) -> Iterable[_GitHubPullRequestLike]:
        """
        Iterate pull requests in a repository via REST.
        """
        gh_repo = self.get_repo(owner=owner, repo=repo)
        pulls = gh_repo.get_pulls(state=state, sort=sort, direction=direction)
        yield from self._iter_with_limit(pulls, limit=limit)

    def iter_issue_comments(
        self, issue: _GitHubIssueLike, *, limit: int | None = None
    ) -> Iterable[object]:
        """
        Iterate comments on an issue via REST.
        """
        yield from self._iter_with_limit(issue.get_comments(), limit=limit)

    def iter_pr_comments(
        self, pr: _GitHubPullRequestLike, *, limit: int | None = None
    ) -> Iterable[object]:
        """
        Iterate comments on a pull request (issue comments + review comments).
        """
        # Issue-style comments
        yield from self.iter_issue_comments(pr, limit=limit)

    def iter_pr_review_comments(
        self, pr: _GitHubPullRequestLike, *, limit: int | None = None
    ) -> Iterable[object]:
        """
        Iterate review comments on a pull request.
        """
        yield from self._iter_with_limit(pr.get_review_comments(), limit=limit)

    def iter_repo_milestones(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        limit: int | None = None,
    ) -> Iterable[object]:
        """
        Iterate milestones in a repository via REST.
        """
        gh_repo = self.get_repo(owner=owner, repo=repo)
        yield from self._iter_with_limit(
            gh_repo.get_milestones(state=state), limit=limit
        )

    def iter_project_v2_items(
        self,
        *,
        org_login: str,
        project_number: int,
        first: int = 50,
        max_items: int | None = None,
    ) -> Iterable[ProjectV2ItemNode]:
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
            with gate_call(self.gate):
                data = self.graphql.query(
                    query,
                    variables={
                        "login": org_login,
                        "number": int(project_number),
                        "after": after,
                        "first": int(max(1, min(100, first))),
                    },
                )

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
                    all_changes: list[dict[str, object]] = []
                    all_changes.extend(changes)
                    changes_cursor = changes_page_info.get("endCursor")

                    # Fetch remaining changes for this specific item
                    while changes_cursor:
                        with gate_call(self.gate):
                            more_changes = self._fetch_item_changes(
                                item_id=item.get("id"),
                                after=changes_cursor,
                            )

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
        after: str | None = None,
    ) -> ProjectItemChanges | None:
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
