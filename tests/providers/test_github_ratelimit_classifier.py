"""Tests for providers/github/ratelimit.py (CHAOS-2773 CS1).

Ports the relevant classification cases from
``tests/test_github_403_observability.py`` (which pins the frozen
``connectors/github.py`` twin of this same triage) onto the extracted
``classify_github_403`` -- the shared classifier
``providers/github/client.py::GitHubWorkClient._raise_github_exception`` now
delegates to, and any future httpx ``GitHubCodeClient`` (CS3+) will too.
"""

from __future__ import annotations

import time

import pytest

from dev_health_ops.exceptions import AuthenticationException, RateLimitException
from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient
from dev_health_ops.providers.github.ratelimit import (
    classify_github_403,
    github_retry_after_seconds,
)
from dev_health_ops.sync.budget_types import BudgetDimension


class TestClassifyGitHub403:
    def test_primary_rate_limit_remaining_zero(self) -> None:
        result = classify_github_403(
            headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "12345"},
            message="API rate limit exceeded",
        )
        assert result.is_rate_limit is True
        assert result.is_primary is True
        assert result.reason == "primary"
        assert result.dimension is BudgetDimension.REST_CORE

    def test_secondary_abuse_via_retry_after_header(self) -> None:
        result = classify_github_403(
            headers={"retry-after": "42"},
            message="You have exceeded a secondary rate limit.",
        )
        assert result.is_rate_limit is True
        assert result.is_primary is False
        assert result.reason == "secondary"
        assert result.dimension is BudgetDimension.SECONDARY_ABUSE_RISK
        assert result.retry_after_seconds == pytest.approx(42.0)

    def test_secondary_abuse_via_body_wording_without_retry_after(self) -> None:
        # GitHub-documented case: the body carries the wording but with NO
        # Retry-After header and NO x-ratelimit-remaining:0 -- must still
        # classify RETRYABLE (secondary), not "not a rate limit".
        result = classify_github_403(
            headers={},
            message="You have exceeded a secondary rate limit. Please wait a few minutes.",
        )
        assert result.is_rate_limit is True
        assert result.reason == "secondary"
        # No Retry-After and no x-ratelimit-reset -- retry_after_seconds is
        # None; the CALLER (GitHubWorkClient) applies its own default wait,
        # mirroring the pre-extraction behavior exactly.
        assert result.retry_after_seconds is None

    def test_abuse_wording_variant(self) -> None:
        result = classify_github_403(headers={}, message="abuse detection triggered")
        assert result.is_rate_limit is True
        assert result.reason == "secondary"

    def test_permission_sso_403_is_not_a_rate_limit(self) -> None:
        result = classify_github_403(
            headers={"x-github-request-id": "REQ:1"},
            message="Resource protected by organization SAML enforcement.",
        )
        assert result.is_rate_limit is False
        assert result.is_primary is False
        assert result.dimension is None
        assert result.reason is None

    def test_bare_permission_403_no_headers_no_wording(self) -> None:
        result = classify_github_403(
            headers={}, message="Resource not accessible by integration"
        )
        assert result.is_rate_limit is False

    def test_primary_wins_over_body_wording_when_both_present(self) -> None:
        # x-ratelimit-remaining: 0 always signals primary regardless of body
        # wording.
        result = classify_github_403(
            headers={"x-ratelimit-remaining": "0"},
            message="unrelated message",
        )
        assert result.is_rate_limit is True
        assert result.is_primary is True
        assert result.reason == "primary"


class TestGithubRetryAfterSeconds:
    def test_prefers_retry_after_header(self) -> None:
        assert github_retry_after_seconds({"retry-after": "30"}) == pytest.approx(30.0)

    def test_falls_back_to_reset_header(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "dev_health_ops.providers.github.ratelimit.time.time", lambda: 100.0
        )
        assert github_retry_after_seconds(
            {"x-ratelimit-reset": "150"}
        ) == pytest.approx(50.0)

    def test_no_headers_returns_none(self) -> None:
        assert github_retry_after_seconds({}) is None

    def test_garbage_retry_after_returns_none(self) -> None:
        assert github_retry_after_seconds({"retry-after": "soon"}) is None


# ---------------------------------------------------------------------------
# GitHubWorkClient._raise_github_exception delegates to the classifier
# (behavior-parity ported from tests/test_github_403_observability.py).
# ---------------------------------------------------------------------------


class _StubGithubException(Exception):
    def __init__(self, status: int, data: dict, headers: dict) -> None:
        super().__init__(str(data))
        self.status = status
        self.data = data
        self.headers = headers


def _client_for_raise() -> GitHubWorkClient:
    # Constructing via __new__ avoids the network-touching __init__.
    # _record_rest_usage defensively getattr()s self._usage (tolerated when
    # absent) but does `self.github.rate_limiting` unconditionally, so
    # `github` itself must at least exist as an attribute (None is fine --
    # getattr(None, "rate_limiting", None) degrades gracefully).
    client = GitHubWorkClient.__new__(GitHubWorkClient)
    client.github = None  # type: ignore[assignment]
    return client


class TestRaiseGithubExceptionDelegation:
    def test_permission_403_raises_non_retryable_auth_error(self) -> None:
        from github import GithubException

        client = _client_for_raise()
        exc = GithubException(
            403,
            {"message": "Resource protected by organization SAML enforcement."},
            {"x-github-request-id": "REQ:42"},
        )
        with pytest.raises(AuthenticationException) as excinfo:
            client._raise_github_exception(exc, operation="GET /repos/o/r")
        assert "REQ:42" in str(excinfo.value)

    def test_primary_rate_limit_403_raises_rate_limit_with_signal(self) -> None:
        from github import GithubException

        client = _client_for_raise()
        exc = GithubException(
            403,
            {"message": "API rate limit exceeded"},
            {
                "x-ratelimit-remaining": "0",
                "x-ratelimit-reset": str(int(time.time()) + 60),
            },
        )
        with pytest.raises(RateLimitException) as excinfo:
            client._raise_github_exception(exc, operation="GET /repos/o/r")
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.reason == "primary"
        assert signal.dimension is BudgetDimension.REST_CORE

    def test_secondary_rate_limit_403_raises_rate_limit_with_signal(self) -> None:
        from github import GithubException

        client = _client_for_raise()
        exc = GithubException(
            403,
            {"message": "You have exceeded a secondary rate limit"},
            {"retry-after": "13"},
        )
        with pytest.raises(RateLimitException) as excinfo:
            client._raise_github_exception(exc, operation="GET /repos/o/r")
        assert excinfo.value.retry_after_seconds == pytest.approx(13.0)
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.reason == "secondary"
        assert signal.dimension is BudgetDimension.SECONDARY_ABUSE_RISK


def test_github_auth_from_credentials_roundtrip() -> None:
    # Smoke check that GitHubAuth (reused by the future GHE base-url join)
    # is still importable/constructible from this module untouched.
    auth = GitHubAuth(token="t", base_url="https://ghe.example.com/api/v3")
    assert auth.base_url == "https://ghe.example.com/api/v3"
    assert auth.is_app_auth is False
