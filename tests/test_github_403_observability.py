"""Diagnosability of GitHub connector 403s (CHAOS-2383).

A real permission/SSO 403 used to surface only as
``Attempt 2/5 failed: Forbidden`` with no endpoint/headers, and the generic
retry decorator spun on it five times. These tests assert that:

- a bare/permission 403 raises a NON-retryable error naming the endpoint and
  the captured GitHub headers (so the retry decorator does not spin),
- rate-limit headers (primary) and Retry-After (secondary/abuse) raise
  ``RateLimitException`` with the correct wait,
- the error/log message includes the endpoint and the diagnostic headers,
- the retry wrapper labels its warnings with the wrapped operation name.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
import requests
from requests.structures import CaseInsensitiveDict

# Initialize the connectors package first to avoid the pre-existing
# providers._base <-> connectors circular import on isolated collection.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.connectors.base import RateLimitException
from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
)
from dev_health_ops.connectors.exceptions import (
    RateLimitException as ExcRateLimitException,
)
from dev_health_ops.connectors.github import GitHubConnector
from dev_health_ops.connectors.utils.graphql import (
    GitHubGraphQLClient,
    safe_github_headers,
)
from dev_health_ops.connectors.utils.retry import retry_with_backoff


class _FakeResponse(requests.Response):
    def __init__(
        self,
        status_code: int,
        headers: dict[str, str] | None = None,
        text: str = "",
    ) -> None:
        super().__init__()
        self.status_code = status_code
        self.headers = CaseInsensitiveDict(headers or {})
        self._content = text.encode()


# --------------------------------------------------------------------------
# safe_github_headers — never leaks the token, captures the diagnostic set
# --------------------------------------------------------------------------


def test_safe_github_headers_filters_to_allowlist_and_drops_token():
    response = _FakeResponse(
        status_code=403,
        headers={
            "Authorization": "Bearer ghs_secret",
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": "12345",
            "Retry-After": "30",
            "X-GitHub-Request-Id": "ABCD:1234",
            "X-Accepted-GitHub-Permissions": "contents=read",
        },
    )
    diag = safe_github_headers(response)

    assert "authorization" not in {k.lower() for k in diag}
    assert diag["x-ratelimit-remaining"] == "0"
    assert diag["x-ratelimit-reset"] == "12345"
    assert diag["retry-after"] == "30"
    assert diag["x-github-request-id"] == "ABCD:1234"
    assert diag["x-accepted-github-permissions"] == "contents=read"


# --------------------------------------------------------------------------
# GraphQL query() — the max_retries=5 path that spun on bare 403s
# --------------------------------------------------------------------------


def _patch_post(monkeypatch, response):
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.graphql.requests.post",
        MagicMock(return_value=response),
    )
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.retry.time.sleep", lambda *_: None
    )


def test_graphql_permission_403_is_nonretryable_and_names_endpoint(monkeypatch):
    post = MagicMock(
        return_value=_FakeResponse(
            status_code=403,
            headers={
                "X-GitHub-Request-Id": "REQ:9",
                "X-Accepted-GitHub-Permissions": "contents=read",
            },
            text="Resource protected by organization SAML enforcement.",
        )
    )
    monkeypatch.setattr("dev_health_ops.connectors.utils.graphql.requests.post", post)
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.retry.time.sleep", lambda *_: None
    )

    client = GitHubGraphQLClient("token")
    with pytest.raises(AuthenticationException) as exc_info:
        client.query("query { viewer { login } }")

    message = str(exc_info.value)
    # Non-retryable: AuthenticationException is not in the retry tuple, so the
    # decorator must NOT have retried -> exactly one HTTP call.
    assert post.call_count == 1
    # Error names the endpoint and surfaces the diagnostic headers.
    assert GitHubGraphQLClient.GRAPHQL_ENDPOINT in message
    assert "x-github-request-id" in message
    assert "REQ:9" in message
    assert "SSO" in message or "permission" in message


def test_graphql_secondary_abuse_403_raises_rate_limit_with_wait(monkeypatch):
    response = _FakeResponse(
        status_code=403,
        headers={"Retry-After": "42", "X-GitHub-Request-Id": "REQ:7"},
        text="You have exceeded a secondary rate limit.",
    )
    _patch_post(monkeypatch, response)

    client = GitHubGraphQLClient("token")
    with pytest.raises(ExcRateLimitException) as exc_info:
        client.query("query { viewer { login } }")

    assert exc_info.value.retry_after_seconds == pytest.approx(42.0)
    assert GitHubGraphQLClient.GRAPHQL_ENDPOINT in str(exc_info.value)


def test_graphql_body_only_secondary_403_is_retryable_without_retry_after(monkeypatch):
    # GitHub-documented case: a secondary/abuse 403 whose body carries the
    # wording but with NO Retry-After header and NO x-ratelimit-remaining:0.
    # The body-wording fallback (mirroring the REST path) must classify it as a
    # RETRYABLE RateLimitException, not a non-retryable AuthenticationException,
    # so the file-contents/blame hot path backs off and retries.
    response = _FakeResponse(
        status_code=403,
        headers={"X-GitHub-Request-Id": "REQ:11"},
        text="You have exceeded a secondary rate limit. Please wait a few minutes.",
    )
    _patch_post(monkeypatch, response)

    client = GitHubGraphQLClient("token")
    with pytest.raises(ExcRateLimitException) as exc_info:
        client.query("query { viewer { login } }")

    # Retryable rate-limit error (not AuthenticationException) with a sane
    # default wait since no Retry-After header was present.
    assert not isinstance(exc_info.value, AuthenticationException)
    assert exc_info.value.retry_after_seconds == pytest.approx(60.0)
    assert GitHubGraphQLClient.GRAPHQL_ENDPOINT in str(exc_info.value)


def test_graphql_primary_rate_limit_403_names_endpoint_and_headers(monkeypatch):
    response = _FakeResponse(
        status_code=403,
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "105"},
        text="API rate limit exceeded",
    )
    _patch_post(monkeypatch, response)
    monkeypatch.setattr(
        "dev_health_ops.connectors.utils.graphql.time.time", lambda: 100
    )

    client = GitHubGraphQLClient("token")
    with pytest.raises(ExcRateLimitException) as exc_info:
        client.query("query { viewer { login } }")

    assert exc_info.value.retry_after_seconds == pytest.approx(5.0)
    message = str(exc_info.value)
    assert GitHubGraphQLClient.GRAPHQL_ENDPOINT in message
    assert "x-ratelimit-remaining" in message


# --------------------------------------------------------------------------
# GitHubConnector._classify_github_403 — REST 403 paths
# --------------------------------------------------------------------------


def test_rest_classify_permission_403_returns_none_and_logs_endpoint(caplog):
    with GitHubConnector(token="t") as connector:
        response = _FakeResponse(
            status_code=403,
            headers={"X-GitHub-Request-Id": "REQ:1"},
            text="Resource not accessible by integration",
        )
        with caplog.at_level(logging.WARNING):
            result = connector._classify_github_403(
                response,
                "GET",
                "/repos/o/r/dependabot/alerts",
            )

    # Permission 403 -> not a rate limit -> caller falls back to "no data".
    assert result is None
    log_text = caplog.text
    assert "/repos/o/r/dependabot/alerts" in log_text
    assert "GET" in log_text
    assert "REQ:1" in log_text
    assert "permission" in log_text.lower() or "sso" in log_text.lower()


def test_rest_classify_primary_rate_limit_returns_rate_limit(monkeypatch):
    with GitHubConnector(token="t") as connector:
        response = _FakeResponse(
            status_code=403,
            headers={"X-RateLimit-Remaining": "0", "Retry-After": "17"},
            text="API rate limit exceeded",
        )
        result = connector._classify_github_403(
            response,
            "GET",
            "/repos/o/r/actions/runs/9/artifacts",
        )

    assert isinstance(result, RateLimitException)
    assert result.retry_after_seconds == pytest.approx(17.0)
    assert "/repos/o/r/actions/runs/9/artifacts" in str(result)


# --------------------------------------------------------------------------
# GitHubConnector._raise_github_403 — PyGithub exception path
# --------------------------------------------------------------------------


def test_pygithub_permission_403_is_nonretryable_auth_error(caplog):
    from github.GithubException import GithubException

    with GitHubConnector(token="t") as connector:
        exc = GithubException(
            403,
            {"message": "Resource protected by organization SAML enforcement."},
            {"x-github-request-id": "REQ:42"},
        )
        with caplog.at_level(logging.WARNING):
            with pytest.raises(AuthenticationException) as exc_info:
                connector._handle_github_exception(exc)

    message = str(exc_info.value)
    assert "REQ:42" in message
    assert "SSO" in message or "permission" in message
    assert "REQ:42" in caplog.text


def test_pygithub_primary_rate_limit_403_raises_rate_limit(monkeypatch):
    from github.GithubException import GithubException

    with GitHubConnector(token="t") as connector:
        monkeypatch.setattr(connector, "_rate_limit_reset_delay_seconds", lambda: 99.0)
        exc = GithubException(
            403,
            {"message": "API rate limit exceeded"},
            {"x-ratelimit-remaining": "0", "x-github-request-id": "REQ:5"},
        )
        with pytest.raises(RateLimitException) as exc_info:
            connector._handle_github_exception(exc)

    assert exc_info.value.retry_after_seconds == pytest.approx(99.0)
    assert "REQ:5" in str(exc_info.value)


# --------------------------------------------------------------------------
# _handle_github_exception — already-classified connector exceptions pass
# through UNCHANGED (the permission/SSO 403 -> retry-spin regression).
# --------------------------------------------------------------------------


def test_handle_github_exception_passes_through_authentication_exception():
    # A non-retryable AuthenticationException (raised by the GraphQL client for
    # a permission/SSO 403) must NOT be re-wrapped into a retryable APIException.
    auth_exc = AuthenticationException("GitHub 403 (permission/SSO): ...")
    with GitHubConnector(token="t") as connector:
        with pytest.raises(AuthenticationException) as exc_info:
            connector._handle_github_exception(auth_exc)

    # Same object, unchanged type — not reclassified to APIException.
    assert exc_info.value is auth_exc
    assert not isinstance(exc_info.value, APIException)


def test_handle_github_exception_normalises_graphql_rate_limit_to_retryable():
    rl_exc = ExcRateLimitException(
        "secondary/abuse rate limit", retry_after_seconds=42.0
    )
    with GitHubConnector(token="t") as connector:
        with pytest.raises(RateLimitException) as exc_info:
            connector._handle_github_exception(rl_exc)

    assert exc_info.value.retry_after_seconds == pytest.approx(42.0)
    assert "secondary/abuse rate limit" in str(exc_info.value)


# --------------------------------------------------------------------------
# retry_with_backoff — operation name is now in the log line
# --------------------------------------------------------------------------


def test_retry_logs_include_operation_name(caplog):
    calls = {"n": 0}

    @retry_with_backoff(
        max_retries=2,
        initial_delay=0.0,
        max_delay=0.0,
        exceptions=(APIException,),
    )
    def flaky_operation() -> str:
        calls["n"] += 1
        raise APIException("boom")

    with caplog.at_level(logging.WARNING):
        with pytest.raises(APIException):
            flaky_operation()

    assert calls["n"] == 2
    # The wrapped function's qualname must appear in the retry warning.
    assert "flaky_operation" in caplog.text
