from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, NoReturn, Protocol, TypedDict, TypeVar, cast
from urllib.parse import urlparse

from urllib3.util.retry import Retry

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.connectors.utils.github_app import GitHubAppTokenProvider
from dev_health_ops.connectors.utils.graphql import GitHubGraphQLClient
from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
    create_rate_limit_gate,
)
from dev_health_ops.credentials.resolver import (
    CredentialResolutionError,
    resolve_credentials_sync,
)
from dev_health_ops.credentials.types import GitHubCredentials
from dev_health_ops.providers._ratelimit import gate_call

logger = logging.getLogger(__name__)


def _github_http_backoff_max() -> float:
    """Return the maximum backoff cap in seconds for GitHub REST retries.

    Reads ``GITHUB_HTTP_BACKOFF_MAX`` from the environment (default 30).
    Always returns at least 1.0 second.
    """
    try:
        return max(1.0, float(os.getenv("GITHUB_HTTP_BACKOFF_MAX", "30")))
    except ValueError:
        return 30.0


def _github_http_retry() -> Retry | int:
    """Return a bounded urllib3 Retry for idempotent GitHub REST reads.

    Only 502/503/504 (transient infrastructure errors) are retried, and only
    for safe methods (GET/HEAD/OPTIONS).  4xx responses — including 403
    rate-limit — are intentionally excluded so RateLimitGate and
    _raise_github_exception keep full ownership of rate-limit semantics.
    Mutations (POST/PATCH/PUT/DELETE) are never retried to avoid double-writes.
    ``raise_on_status=False`` lets PyGithub surface the final 5xx as its
    normal GithubException rather than a urllib3 MaxRetryError.

    ``respect_retry_after_header=False`` is intentional: urllib3 v2's
    ``is_retry()`` retries 413/429/503 when they carry a ``Retry-After``
    header and ``respect_retry_after_header=True``, even if those codes are
    not in ``status_forcelist``.  That would silently transport-retry GitHub
    secondary-rate-limit 429s instead of letting them surface as
    ``RateLimitException`` for the worker deferral path, and would sleep for
    an unbounded ``Retry-After`` (urllib3 default ``retry_after_max`` ~6 h)
    inside a single ``RateLimitGate`` call, outside the socket timeout.
    With ``respect_retry_after_header=False`` only the codes in
    ``status_forcelist`` are retried, using bounded exponential backoff
    (capped at ``backoff_max``) — never a ``Retry-After`` sleep.
    """
    try:
        total = int(os.getenv("GITHUB_HTTP_MAX_RETRIES", "3"))
    except ValueError:
        total = 3
    if total <= 0:
        return 0
    try:
        backoff = float(os.getenv("GITHUB_HTTP_BACKOFF_FACTOR", "1.0"))
    except ValueError:
        backoff = 1.0
    return Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        status_forcelist=(502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
        backoff_factor=backoff,
        backoff_max=_github_http_backoff_max(),
        respect_retry_after_header=False,
        raise_on_status=False,
    )


def _github_http_timeout() -> int:
    """Return the HTTP timeout in seconds for PyGithub REST calls.

    Reads ``GITHUB_HTTP_TIMEOUT_SECONDS`` from the environment (default 30).
    Always returns at least 1 second.
    """
    try:
        return max(1, int(os.getenv("GITHUB_HTTP_TIMEOUT_SECONDS", "30")))
    except ValueError:
        return 30


_DIAGNOSTIC_HEADER_NAMES = (
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "x-ratelimit-used",
    "x-ratelimit-resource",
    "retry-after",
    "x-github-request-id",
    "x-accepted-github-permissions",
)
_MAX_USAGE_OBSERVATION_KEYS = 50

_TItem = TypeVar("_TItem")


_TIMELINE_TYPENAME_TO_EVENT = {
    "MergedEvent": "merged",
    "ClosedEvent": "closed",
    "ReopenedEvent": "reopened",
}


def _parse_github_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _diagnostic_headers(headers: object) -> dict[str, str]:
    if not isinstance(headers, dict):
        return {}
    lowered = {str(k).lower(): str(v) for k, v in headers.items()}
    return {name: lowered[name] for name in _DIAGNOSTIC_HEADER_NAMES if name in lowered}


def _github_error_message(data: object) -> str:
    if isinstance(data, dict):
        message = data.get("message")
        if message is not None:
            return str(message)
    if data is None:
        return ""
    return str(data)


def _retry_after_seconds(headers: dict[str, str]) -> float | None:
    retry_after = headers.get("retry-after")
    if retry_after is not None:
        try:
            return float(retry_after)
        except ValueError:
            return None

    reset = headers.get("x-ratelimit-reset")
    if reset is not None:
        try:
            return max(0.0, float(reset) - time.time())
        except ValueError:
            return None
    return None


class _GitHubLabelLike(Protocol):
    name: object


class _GitHubUserLike(Protocol):
    email: object
    login: object
    name: object


@dataclass
class GitHubGraphQLUser:
    login: object = None
    email: object = None
    name: object = None


@dataclass
class GitHubGraphQLComment:
    id: object
    created_at: object
    user: object
    body: object


@dataclass
class GitHubGraphQLReview:
    id: object
    reviewer: object
    state: object
    submitted_at: object
    body: object
    url: object


@dataclass
class GitHubGraphQLEvent:
    created_at: object
    event: object
    actor: object = None
    label: object = None


@dataclass(frozen=True)
class BatchedPRPayload:
    number: int
    issue_comments: tuple[GitHubGraphQLComment, ...]
    review_comments: tuple[GitHubGraphQLComment, ...]
    reviews: tuple[GitHubGraphQLReview, ...]
    events: tuple[GitHubGraphQLEvent, ...] = ()


class _GitHubEventLike(Protocol):
    created_at: object
    event: object
    label: object
    actor: object


class _GitHubCommentLike(Protocol):
    id: object
    created_at: object
    user: object
    body: object


class _GitHubMilestoneLike(Protocol):
    id: object
    number: object
    title: object
    created_at: object
    due_on: object
    state: object


class _GitHubIssueBaseLike(Protocol):
    number: object
    title: object
    body: object
    state: object
    created_at: object
    updated_at: object
    closed_at: object
    labels: object
    assignees: object
    user: object
    html_url: object
    url: object
    pull_request: object

    def get_comments(self) -> Iterable[_GitHubCommentLike]: ...


class _GitHubIssueLike(_GitHubIssueBaseLike, Protocol):
    def get_events(self) -> Iterable[_GitHubEventLike]: ...


class _GitHubPullRequestLike(_GitHubIssueBaseLike, Protocol):
    merged: object
    merged_at: object
    draft: object

    # PyGithub's PullRequest exposes the issue-events endpoint as
    # get_issue_events(); only Issue has get_events().
    def get_issue_events(self) -> Iterable[_GitHubEventLike]:
        raise NotImplementedError

    def get_review_comments(self) -> Iterable[_GitHubCommentLike]: ...


class _GitHubRepositoryLike(Protocol):
    def get_issues(
        self, *args: object, **kwargs: object
    ) -> Iterable[_GitHubIssueLike]: ...

    def get_pulls(
        self, *args: object, **kwargs: object
    ) -> Iterable[_GitHubPullRequestLike]: ...

    def get_milestones(
        self, *args: object, **kwargs: object
    ) -> Iterable[_GitHubMilestoneLike]: ...


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
        org_id: str | None = None,
    ) -> None:
        from github import Auth, Github  # PyGithub

        self.auth = auth
        self.per_page = max(1, min(100, int(per_page)))
        base_url = auth.base_url or "https://api.github.com"
        host = urlparse(base_url).hostname or "api.github.com"
        self.gate = gate or create_rate_limit_gate(
            "github",
            org_id=org_id,
            host=host,
            config=RateLimitConfig(initial_backoff_seconds=1.0),
        )
        self._usage_observations: dict[tuple[str, str], dict[str, Any]] = {}
        self._usage_observation_overflow = 0
        self._app_token_provider: GitHubAppTokenProvider | None = None

        token = auth.token
        pygithub_auth: Any | None = None
        if auth.is_app_auth:
            assert auth.app_id is not None
            assert auth.private_key is not None
            assert auth.installation_id is not None
            app_auth = Auth.AppAuth(auth.app_id, auth.private_key)
            pygithub_auth = Auth.AppInstallationAuth(
                app_auth, int(auth.installation_id)
            )
            self._app_token_provider = GitHubAppTokenProvider(
                app_id=auth.app_id,
                private_key=auth.private_key,
                installation_id=auth.installation_id,
                api_base_url=base_url,
            )
            token = self._app_token_provider.get_token()
        elif token:
            pygithub_auth = Auth.Token(token)
        if not token:
            raise ValueError("GitHubWorkClient requires token or GitHub App auth")

        if auth.base_url:
            self.github = Github(
                base_url=auth.base_url,
                auth=pygithub_auth,
                per_page=self.per_page,
                retry=_github_http_retry(),
                timeout=_github_http_timeout(),
            )
        else:
            self.github = Github(
                auth=pygithub_auth,
                per_page=self.per_page,
                retry=_github_http_retry(),
                timeout=_github_http_timeout(),
            )

        # GraphQL client (api.github.com only for now).
        token_provider = (
            self._app_token_provider.get_token
            if self._app_token_provider is not None
            else None
        )
        self.graphql = GitHubGraphQLClient(token, token_provider=token_provider)

    @classmethod
    def from_env(cls, *, org_id: str | None = None) -> GitHubWorkClient:
        try:
            credentials = resolve_credentials_sync("github", allow_env_fallback=True)
        except CredentialResolutionError as exc:
            raise ValueError(
                "GITHUB_TOKEN environment variable is required (or configure GitHub App "
                "credentials via GITHUB_APP_ID/GITHUB_APP_PRIVATE_KEY_PATH/"
                "GITHUB_APP_INSTALLATION_ID, or store credentials in the database)."
            ) from exc
        if not isinstance(credentials, GitHubCredentials):
            raise ValueError("Resolved credentials are not GitHub credentials")
        return cls(auth=GitHubAuth.from_credentials(credentials), org_id=org_id)

    def get_repo(self, *, owner: str, repo: str) -> Any:
        operation = f"GET /repos/{owner}/{repo}"
        return self._call_github(
            operation, lambda: self.github.get_repo(f"{owner}/{repo}")
        )

    def _call_github(self, operation: str, call: Callable[[], _TItem]) -> _TItem:
        with gate_call(self.gate):
            try:
                result = call()
                self._record_rest_usage(operation)
                return result
            except Exception as exc:
                self._raise_github_exception(exc, operation=operation)

    def _query_graphql(
        self,
        operation: str,
        query: str,
        *,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = self.graphql.query(query, variables=variables)
        headers = getattr(self.graphql, "last_response_headers", {})
        status = getattr(self.graphql, "last_response_status", None)
        rate_limit_data = getattr(self.graphql, "last_rate_limit_data", None)
        rate_limit = dict(rate_limit_data) if isinstance(rate_limit_data, dict) else {}
        self._record_usage_observation(
            transport="graphql",
            operation=operation,
            headers=_diagnostic_headers(headers),
            rate_limit=rate_limit,
            status=status if isinstance(status, int) else None,
        )
        return data

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
        observations = getattr(self, "_usage_observations", None)
        if observations is None:
            observations = {}
            self._usage_observations = observations
        observation = observations.get(key)
        if observation is None:
            if len(observations) >= _MAX_USAGE_OBSERVATION_KEYS:
                self._usage_observation_overflow = (
                    getattr(self, "_usage_observation_overflow", 0) + 1
                )
                return
            observation = {
                "transport": transport,
                "operation": operation,
                "request_count": 0,
            }
            observations[key] = observation
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
        headers: dict[str, str] | None = None,
        status: int | None = None,
    ) -> None:
        safe_headers = _diagnostic_headers(headers or {})
        rate_limit: dict[str, Any] = {}
        remaining = safe_headers.get("x-ratelimit-remaining")
        reset = safe_headers.get("x-ratelimit-reset")
        limit = safe_headers.get("x-ratelimit-limit")
        used = safe_headers.get("x-ratelimit-used")
        resource = safe_headers.get("x-ratelimit-resource")

        if remaining is None or limit is None:
            rate_limiting = getattr(self.github, "rate_limiting", None)
            if isinstance(rate_limiting, Sequence) and len(rate_limiting) >= 2:
                remaining = remaining or str(rate_limiting[0])
                limit = limit or str(rate_limiting[1])
        if reset is None:
            reset_time = getattr(self.github, "rate_limiting_resettime", None)
            if reset_time is not None:
                reset = str(reset_time)

        for name, value in {
            "remaining": remaining,
            "reset": reset,
            "limit": limit,
            "used": used,
            "resource": resource,
        }.items():
            if value is not None:
                rate_limit[name] = value
        self._record_usage_observation(
            transport="rest",
            operation=operation,
            headers=safe_headers,
            rate_limit=rate_limit,
            status=status,
        )

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        usage_observations = getattr(self, "_usage_observations", {})
        observations = [dict(value) for value in usage_observations.values()]
        overflow = getattr(self, "_usage_observation_overflow", 0)
        if overflow:
            observations.append(
                {
                    "transport": "summary",
                    "operation": "overflow",
                    "dropped_operation_count": overflow,
                }
            )
        usage_observations.clear()
        self._usage_observations = usage_observations
        self._usage_observation_overflow = 0
        return observations

    def _raise_github_exception(self, exc: Exception, *, operation: str) -> NoReturn:
        from github import GithubException, RateLimitExceededException

        if isinstance(exc, (APIException, AuthenticationException, NotFoundException)):
            raise exc
        if isinstance(exc, RateLimitException):
            raise exc
        if isinstance(exc, RateLimitExceededException):
            self._record_rest_usage(operation)
            raise RateLimitException(
                f"GitHub rate limit on {operation}: {exc}",
                retry_after_seconds=self._rate_limit_reset_delay_seconds(),
            )
        if not isinstance(exc, GithubException):
            raise APIException(f"GitHub API error on {operation}: {exc}") from exc

        status = getattr(exc, "status", None)
        headers = _diagnostic_headers(getattr(exc, "headers", None))
        self._record_rest_usage(
            operation,
            headers=headers,
            status=status if isinstance(status, int) else None,
        )
        message = _github_error_message(getattr(exc, "data", None))
        if status == 401:
            raise AuthenticationException(
                f"GitHub authentication failed on {operation}: {message} (headers={headers})"
            ) from exc
        if status == 404:
            raise NotFoundException(
                f"GitHub resource not found on {operation}: {message} (headers={headers})"
            ) from exc
        if status == 403:
            lowered = message.lower()
            is_rate_limit = (
                headers.get("x-ratelimit-remaining") == "0"
                or "retry-after" in headers
                or "rate limit" in lowered
                or "abuse" in lowered
                or "secondary" in lowered
            )
            if is_rate_limit:
                retry_after = _retry_after_seconds(headers)
                logger.warning(
                    "GitHub rate limit (403) on %s headers=%s message=%s",
                    operation,
                    headers,
                    message,
                )
                raise RateLimitException(
                    f"GitHub rate limit (403) on {operation}: {message} (headers={headers})",
                    retry_after_seconds=retry_after,
                ) from exc
            logger.warning(
                "GitHub 403 on %s headers=%s message=%s",
                operation,
                headers,
                message,
            )
            raise AuthenticationException(
                f"GitHub 403 on {operation}: {message} (headers={headers})"
            ) from exc
        raise APIException(
            f"GitHub API error on {operation}: HTTP {status} {message} (headers={headers})"
        ) from exc

    def _rate_limit_reset_delay_seconds(self) -> float | None:
        reset = getattr(self.github, "rate_limiting_resettime", None)
        if reset is None:
            return None
        try:
            return max(0.0, float(reset) - time.time())
        except (TypeError, ValueError):
            return None

    def _iter_with_limit(
        self,
        source: Iterable[_TItem],
        *,
        limit: int | None,
        skip: Callable[[_TItem], bool] | None = None,
    ) -> Iterable[_TItem]:
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

    def _iter_github_items(
        self,
        source: Iterable[_TItem],
        *,
        operation: str,
        limit: int | None,
        skip: Callable[[_TItem], bool] | None = None,
    ) -> Iterable[_TItem]:
        with gate_call(self.gate):
            try:
                yield from self._iter_with_limit(source, limit=limit, skip=skip)
                self._record_rest_usage(operation)
            except Exception as exc:
                self._raise_github_exception(exc, operation=operation)

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
        operation = f"GET /repos/{owner}/{repo}/issues"
        issues = self._call_github(
            operation, lambda: gh_repo.get_issues(state=state, since=since)
        )
        yield from self._iter_github_items(
            issues,
            operation=operation,
            limit=limit,
            skip=lambda issue: getattr(issue, "pull_request", None) is not None,
        )

    def iter_issue_events(
        self,
        issue: _GitHubIssueLike | _GitHubPullRequestLike,
        *,
        limit: int | None = None,
    ) -> Iterable[_GitHubEventLike]:
        """
        Iterate issue events (labeled/unlabeled/closed/reopened/assigned/...) via REST.

        Accepts both Issues and PullRequests: PyGithub's Issue exposes the
        endpoint as get_events(), PullRequest as get_issue_events().
        """
        issue_number = getattr(issue, "number", "?")
        operation = f"GET issue events for #{issue_number}"
        get_events = getattr(issue, "get_events", None)
        if callable(get_events):
            events = self._call_github(operation, get_events)
        else:
            get_issue_events = getattr(issue, "get_issue_events")
            events = self._call_github(operation, get_issue_events)
        yield from self._iter_github_items(
            cast(Iterable[_GitHubEventLike], events),
            operation=operation,
            limit=limit,
        )

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
        operation = f"GET /repos/{owner}/{repo}/pulls"
        pulls = self._call_github(
            operation,
            lambda: gh_repo.get_pulls(state=state, sort=sort, direction=direction),
        )
        yield from self._iter_github_items(pulls, operation=operation, limit=limit)

    def iter_issue_comments(
        self, issue: _GitHubIssueBaseLike, *, limit: int | None = None
    ) -> Iterable[_GitHubCommentLike]:
        """
        Iterate comments on an issue via REST.
        """
        issue_number = getattr(issue, "number", "?")
        operation = f"GET issue comments for #{issue_number}"
        comments = self._call_github(operation, issue.get_comments)
        yield from self._iter_github_items(comments, operation=operation, limit=limit)

    def iter_pr_comments(
        self, pr: _GitHubPullRequestLike, *, limit: int | None = None
    ) -> Iterable[_GitHubCommentLike]:
        """
        Iterate comments on a pull request (issue comments + review comments).
        """
        # Issue-style comments
        yield from self.iter_issue_comments(pr, limit=limit)

    def iter_pr_review_comments(
        self, pr: _GitHubPullRequestLike, *, limit: int | None = None
    ) -> Iterable[_GitHubCommentLike]:
        """
        Iterate review comments on a pull request.
        """
        pr_number = getattr(pr, "number", "?")
        operation = f"GET pull request review comments for #{pr_number}"
        comments = self._call_github(operation, pr.get_review_comments)
        yield from self._iter_github_items(comments, operation=operation, limit=limit)

    def iter_pr_social_data_batch(
        self,
        *,
        owner: str,
        repo: str,
        prs: Sequence[_GitHubPullRequestLike],
        comments_limit: int | None = None,
        review_comments_limit: int | None = None,
        reviews_limit: int | None = None,
        events_limit: int | None = 0,
        batch_size: int = 50,
    ) -> Iterable[BatchedPRPayload]:
        """Fetch PR issue comments, reviews, and review comments in GraphQL batches.

        The query uses ``repository.pullRequest(number: ...)`` aliases instead of
        ``repository.pullRequests`` pagination because callers already iterate PRs
        through PyGithub and pass concrete PR objects. Aliases let us preserve that
        iterator contract while collapsing N per-PR REST calls into one GraphQL
        query per up-to-50 PRs. Nested connections are page-limited to control
        GraphQL cost and are paginated per PR only when the first page indicates
        more data.
        """
        numbers: list[int] = []
        for pr in prs:
            number = int(getattr(pr, "number", 0) or 0)
            if number > 0:
                numbers.append(number)

        if not numbers:
            return

        page_size = max(1, min(50, int(batch_size)))
        for index in range(0, len(numbers), page_size):
            chunk = numbers[index : index + page_size]
            initial = self._fetch_pr_social_data_page(
                owner=owner,
                repo=repo,
                numbers=chunk,
                comments_first=self._connection_first(comments_limit),
                review_comments_first=self._connection_first(review_comments_limit),
                reviews_first=self._connection_first(reviews_limit),
                events_first=self._connection_first(events_limit),
            )
            yield from self._complete_pr_social_payloads(
                owner=owner,
                repo=repo,
                initial=initial,
                comments_limit=comments_limit,
                review_comments_limit=review_comments_limit,
                reviews_limit=reviews_limit,
                events_limit=events_limit,
            )

    def iter_pr_comments_batch(
        self,
        *,
        owner: str,
        repo: str,
        prs: Sequence[_GitHubPullRequestLike],
        limit: int | None = None,
    ) -> Iterable[tuple[int, tuple[GitHubGraphQLComment, ...]]]:
        """Return issue-style PR comments keyed by PR number."""
        for payload in self.iter_pr_social_data_batch(
            owner=owner,
            repo=repo,
            prs=prs,
            comments_limit=limit,
            review_comments_limit=0,
            reviews_limit=0,
        ):
            yield payload.number, payload.issue_comments

    def iter_pr_review_comments_batch(
        self,
        *,
        owner: str,
        repo: str,
        prs: Sequence[_GitHubPullRequestLike],
        limit: int | None = None,
    ) -> Iterable[tuple[int, tuple[GitHubGraphQLComment, ...]]]:
        """Return review comments keyed by PR number."""
        for payload in self.iter_pr_social_data_batch(
            owner=owner,
            repo=repo,
            prs=prs,
            comments_limit=0,
            review_comments_limit=limit,
            reviews_limit=limit,
        ):
            yield payload.number, payload.review_comments

    def iter_pr_reviews_batch(
        self,
        *,
        owner: str,
        repo: str,
        prs: Sequence[_GitHubPullRequestLike],
        limit: int | None = None,
    ) -> Iterable[tuple[int, tuple[GitHubGraphQLReview, ...]]]:
        """Return reviews keyed by PR number."""
        for payload in self.iter_pr_social_data_batch(
            owner=owner,
            repo=repo,
            prs=prs,
            comments_limit=0,
            review_comments_limit=0,
            reviews_limit=limit,
        ):
            yield payload.number, payload.reviews

    @staticmethod
    def _connection_first(limit: int | None) -> int:
        if limit is not None and int(limit) <= 0:
            return 0
        if limit is None:
            return 100
        return max(1, min(100, int(limit)))

    @staticmethod
    def _remaining_limit(limit: int | None, current_count: int) -> int | None:
        if limit is None:
            return None
        return max(0, int(limit) - current_count)

    @staticmethod
    def _connection_nodes(connection: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not connection:
            return []
        nodes = connection.get("nodes") or []
        return [node for node in nodes if isinstance(node, dict)]

    @staticmethod
    def _connection_cursor(connection: dict[str, Any] | None) -> str | None:
        page_info = (connection or {}).get("pageInfo") or {}
        if not isinstance(page_info, dict) or not page_info.get("hasNextPage"):
            return None
        cursor = page_info.get("endCursor")
        return cursor if isinstance(cursor, str) and cursor else None

    @staticmethod
    def _graphql_arg(value: object) -> str:
        return json.dumps(value)

    @classmethod
    def _comment_from_graphql(cls, node: dict[str, Any]) -> GitHubGraphQLComment:
        author = node.get("author") if isinstance(node.get("author"), dict) else {}
        user = GitHubGraphQLUser(login=(author or {}).get("login"))
        raw_id = node.get("databaseId") or node.get("fullDatabaseId") or node.get("id")
        return GitHubGraphQLComment(
            id=raw_id,
            created_at=_parse_github_datetime(node.get("createdAt")),
            user=user,
            body=node.get("body") or "",
        )

    @classmethod
    def _review_from_graphql(cls, node: dict[str, Any]) -> GitHubGraphQLReview:
        author = node.get("author") if isinstance(node.get("author"), dict) else {}
        raw_id = node.get("databaseId") or node.get("fullDatabaseId") or node.get("id")
        return GitHubGraphQLReview(
            id=raw_id,
            reviewer=(author or {}).get("login") or "Unknown",
            state=node.get("state") or "",
            submitted_at=_parse_github_datetime(node.get("submittedAt")),
            body=node.get("body") or "",
            url=node.get("url"),
        )

    @classmethod
    def _event_from_graphql(cls, node: dict[str, Any]) -> GitHubGraphQLEvent:
        actor_node = node.get("actor") if isinstance(node.get("actor"), dict) else {}
        actor = GitHubGraphQLUser(login=(actor_node or {}).get("login"))
        typename = str(node.get("__typename") or "")
        return GitHubGraphQLEvent(
            created_at=_parse_github_datetime(node.get("createdAt")),
            event=_TIMELINE_TYPENAME_TO_EVENT.get(typename, ""),
            actor=actor,
            label=None,
        )

    def _fetch_pr_social_data_page(
        self,
        *,
        owner: str,
        repo: str,
        numbers: Sequence[int],
        comments_first: int,
        review_comments_first: int,
        reviews_first: int,
        comments_after: str | None = None,
        reviews_after: str | None = None,
        events_first: int = 0,
        events_after: str | None = None,
    ) -> dict[int, dict[str, Any]]:
        aliases: list[str] = []
        for idx, number in enumerate(numbers):
            fields = ["number"]
            if comments_first > 0:
                comments_after_arg = self._graphql_arg(comments_after)
                fields.append(
                    f"comments(first: {comments_first}, after: {comments_after_arg}, orderBy: {{field: UPDATED_AT, direction: ASC}}) "
                    "{ nodes { id databaseId fullDatabaseId body createdAt author { login } } "
                    "pageInfo { hasNextPage endCursor } }"
                )
            if reviews_first > 0:
                review_fields = [
                    "id databaseId fullDatabaseId body state submittedAt url author { login }"
                ]
                if review_comments_first > 0:
                    review_fields.append(
                        f"comments(first: {review_comments_first}) "
                        "{ nodes { id databaseId fullDatabaseId body createdAt author { login } } "
                        "pageInfo { hasNextPage endCursor } }"
                    )
                reviews_after_arg = self._graphql_arg(reviews_after)
                fields.append(
                    f"reviews(first: {reviews_first}, after: {reviews_after_arg}) "
                    f"{{ nodes {{ {' '.join(review_fields)} }} pageInfo {{ hasNextPage endCursor }} }}"
                )
            if events_first > 0:
                events_after_arg = self._graphql_arg(events_after)
                fields.append(
                    "timelineItems(itemTypes: [MERGED_EVENT, CLOSED_EVENT, "
                    "REOPENED_EVENT], "
                    f"first: {events_first}, after: {events_after_arg}) "
                    "{ nodes { __typename "
                    "... on MergedEvent { createdAt actor { login } } "
                    "... on ClosedEvent { createdAt actor { login } } "
                    "... on ReopenedEvent { createdAt actor { login } } } "
                    "pageInfo { hasNextPage endCursor } }"
                )
            aliases.append(
                f"pr{idx}: pullRequest(number: {int(number)}) {{ {' '.join(fields)} }}"
            )

        aliases_query = "\n".join(aliases)
        query = f"""
        query($owner: String!, $repo: String!) {{
          repository(owner: $owner, name: $repo) {{
            {aliases_query}
          }}
        }}
        """
        with gate_call(self.gate):
            data = self._query_graphql(
                "POST /graphql PR social data",
                query,
                variables={"owner": owner, "repo": repo},
            )
        repository = (data or {}).get("repository") or {}
        result: dict[int, dict[str, Any]] = {}
        for idx, number in enumerate(numbers):
            pr_node = repository.get(f"pr{idx}") or {}
            if isinstance(pr_node, dict):
                result[int(number)] = pr_node
        return result

    def _fetch_review_comments_page(
        self,
        *,
        review_id: str,
        first: int,
        after: str | None,
    ) -> dict[str, Any] | None:
        query = """
        query($reviewId: ID!, $first: Int!, $after: String) {
          node(id: $reviewId) {
            ... on PullRequestReview {
              comments(first: $first, after: $after) {
                nodes { id databaseId fullDatabaseId body createdAt author { login } }
                pageInfo { hasNextPage endCursor }
              }
            }
          }
        }
        """
        with gate_call(self.gate):
            data = self._query_graphql(
                "POST /graphql PR review comments",
                query,
                variables={"reviewId": review_id, "first": first, "after": after},
            )
        node = (data or {}).get("node") or {}
        comments = node.get("comments") if isinstance(node, dict) else None
        return comments if isinstance(comments, dict) else None

    def _complete_pr_social_payloads(
        self,
        *,
        owner: str,
        repo: str,
        initial: dict[int, dict[str, Any]],
        comments_limit: int | None,
        review_comments_limit: int | None,
        reviews_limit: int | None,
        events_limit: int | None = 0,
    ) -> Iterable[BatchedPRPayload]:
        for number, pr_node in initial.items():
            comments_connection = pr_node.get("comments")
            comments = [
                self._comment_from_graphql(node)
                for node in self._connection_nodes(comments_connection)
            ]
            comments_cursor = self._connection_cursor(comments_connection)
            while (
                comments_cursor
                and self._remaining_limit(comments_limit, len(comments)) != 0
            ):
                remaining = self._remaining_limit(comments_limit, len(comments))
                more = self._fetch_pr_social_data_page(
                    owner=owner,
                    repo=repo,
                    numbers=[number],
                    comments_first=self._connection_first(remaining),
                    review_comments_first=0,
                    reviews_first=0,
                    comments_after=comments_cursor,
                ).get(number, {})
                more_connection = (
                    more.get("comments") if isinstance(more, dict) else None
                )
                comments.extend(
                    self._comment_from_graphql(node)
                    for node in self._connection_nodes(more_connection)
                )
                comments_cursor = self._connection_cursor(more_connection)

            reviews_connection = pr_node.get("reviews")
            reviews_nodes = self._connection_nodes(reviews_connection)
            reviews_cursor = self._connection_cursor(reviews_connection)
            while (
                reviews_cursor
                and self._remaining_limit(reviews_limit, len(reviews_nodes)) != 0
            ):
                remaining_reviews = self._remaining_limit(
                    reviews_limit, len(reviews_nodes)
                )
                more = self._fetch_pr_social_data_page(
                    owner=owner,
                    repo=repo,
                    numbers=[number],
                    comments_first=0,
                    review_comments_first=self._connection_first(review_comments_limit),
                    reviews_first=self._connection_first(remaining_reviews),
                    reviews_after=reviews_cursor,
                ).get(number, {})
                more_reviews = more.get("reviews") if isinstance(more, dict) else None
                reviews_nodes.extend(self._connection_nodes(more_reviews))
                reviews_cursor = self._connection_cursor(more_reviews)

            reviews = [self._review_from_graphql(node) for node in reviews_nodes]
            review_comments: list[GitHubGraphQLComment] = []
            for review_node in reviews_nodes:
                review_comment_connection = review_node.get("comments")
                review_comments.extend(
                    self._comment_from_graphql(node)
                    for node in self._connection_nodes(review_comment_connection)
                )
                review_comment_cursor = self._connection_cursor(
                    review_comment_connection
                )
                review_node_id = review_node.get("id")
                while (
                    isinstance(review_node_id, str)
                    and review_comment_cursor
                    and self._remaining_limit(
                        review_comments_limit, len(review_comments)
                    )
                    != 0
                ):
                    remaining_comments = self._remaining_limit(
                        review_comments_limit, len(review_comments)
                    )
                    more_comments = self._fetch_review_comments_page(
                        review_id=review_node_id,
                        first=self._connection_first(remaining_comments),
                        after=review_comment_cursor,
                    )
                    review_comments.extend(
                        self._comment_from_graphql(node)
                        for node in self._connection_nodes(more_comments)
                    )
                    review_comment_cursor = self._connection_cursor(more_comments)

            if comments_limit is not None:
                comments = comments[: int(comments_limit)]
            if reviews_limit is not None:
                reviews = reviews[: int(reviews_limit)]
            if review_comments_limit is not None:
                review_comments = review_comments[: int(review_comments_limit)]

            events_connection = pr_node.get("timelineItems")
            events_nodes = self._connection_nodes(events_connection)
            events_cursor = self._connection_cursor(events_connection)
            while (
                events_cursor
                and self._remaining_limit(events_limit, len(events_nodes)) != 0
            ):
                remaining_events = self._remaining_limit(
                    events_limit, len(events_nodes)
                )
                more = self._fetch_pr_social_data_page(
                    owner=owner,
                    repo=repo,
                    numbers=[number],
                    comments_first=0,
                    review_comments_first=0,
                    reviews_first=0,
                    events_first=self._connection_first(remaining_events),
                    events_after=events_cursor,
                ).get(number, {})
                more_events = (
                    more.get("timelineItems") if isinstance(more, dict) else None
                )
                events_nodes.extend(self._connection_nodes(more_events))
                events_cursor = self._connection_cursor(more_events)
            events = [self._event_from_graphql(node) for node in events_nodes]
            if events_limit is not None:
                events = events[: int(events_limit)]

            yield BatchedPRPayload(
                number=number,
                issue_comments=tuple(comments),
                review_comments=tuple(review_comments),
                reviews=tuple(reviews),
                events=tuple(events),
            )

    def iter_repo_milestones(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        limit: int | None = None,
    ) -> Iterable[_GitHubMilestoneLike]:
        """
        Iterate milestones in a repository via REST.
        """
        gh_repo = self.get_repo(owner=owner, repo=repo)
        operation = f"GET /repos/{owner}/{repo}/milestones"
        milestones = self._call_github(
            operation, lambda: gh_repo.get_milestones(state=state)
        )
        yield from self._iter_github_items(milestones, operation=operation, limit=limit)

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
                data = self._query_graphql(
                    "POST /graphql project v2 items",
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
                    raw_changes_cursor = changes_page_info.get("endCursor")
                    changes_cursor = (
                        raw_changes_cursor
                        if isinstance(raw_changes_cursor, str)
                        else None
                    )

                    # Fetch remaining changes for this specific item
                    while changes_cursor:
                        with gate_call(self.gate):
                            more_changes = self._fetch_item_changes(
                                item_id=str(item.get("id")),
                                after=changes_cursor,
                            )

                        if not more_changes or not more_changes.get("nodes"):
                            break

                        all_changes.extend(more_changes.get("nodes") or [])
                        changes_page_info = more_changes.get("pageInfo") or {}
                        raw_changes_cursor = changes_page_info.get("endCursor")
                        changes_cursor = (
                            raw_changes_cursor
                            if isinstance(raw_changes_cursor, str)
                            else None
                        )

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

        data = self._query_graphql(
            "POST /graphql project v2 item changes",
            query,
            variables={
                "itemId": item_id,
                "after": after,
            },
        )

        node = (data or {}).get("node") or {}
        return node.get("changes")
