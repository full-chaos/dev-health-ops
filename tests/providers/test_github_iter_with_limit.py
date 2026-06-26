"""Test GitHubWorkClient._iter_with_limit generic helper and HTTP retry/timeout helpers."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient


@pytest.fixture
def client() -> GitHubWorkClient:
    with (
        patch("github.Github"),
        patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
    ):
        return GitHubWorkClient(auth=GitHubAuth(token="fake"))


class TestIterWithLimit:
    def test_no_limit_yields_all(self, client: GitHubWorkClient) -> None:
        source = [MagicMock(), MagicMock(), MagicMock()]
        result = list(client._iter_with_limit(source, limit=None))
        assert result == source

    def test_limit_truncates(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5]
        result = list(client._iter_with_limit(source, limit=3))
        assert result == [1, 2, 3]

    def test_limit_zero_yields_none(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3]
        result = list(client._iter_with_limit(source, limit=0))
        assert result == []

    def test_limit_larger_than_source(self, client: GitHubWorkClient) -> None:
        source = [1, 2]
        result = list(client._iter_with_limit(source, limit=10))
        assert result == [1, 2]

    def test_custom_filter_skips_items(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5]

        def skip_evens(x: Any) -> bool:
            return x % 2 == 0  # predicate returns True when item should be SKIPPED

        result = list(client._iter_with_limit(source, limit=None, skip=skip_evens))
        assert result == [1, 3, 5]

    def test_filter_plus_limit(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5, 6]

        def skip_evens(x: Any) -> bool:
            return x % 2 == 0

        result = list(client._iter_with_limit(source, limit=2, skip=skip_evens))
        assert result == [1, 3]


# ============================================================================
# HTTP retry / timeout helpers
# ============================================================================


class TestGithubHttpRetry:
    def test_returns_retry_with_correct_status_forcelist(self) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert set(result.status_forcelist) == {502, 503, 504}

    def test_allowed_methods_are_safe_only(self) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.allowed_methods == frozenset({"GET", "HEAD", "OPTIONS"})

    def test_env_override_max_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        monkeypatch.setenv("GITHUB_HTTP_MAX_RETRIES", "5")
        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.total == 5

    def test_env_override_backoff_factor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        monkeypatch.setenv("GITHUB_HTTP_BACKOFF_FACTOR", "2.5")
        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.backoff_factor == 2.5

    def test_zero_or_negative_max_retries_returns_int_zero(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dev_health_ops.providers.github.client import _github_http_retry

        monkeypatch.setenv("GITHUB_HTTP_MAX_RETRIES", "0")
        assert _github_http_retry() == 0

        monkeypatch.setenv("GITHUB_HTTP_MAX_RETRIES", "-1")
        assert _github_http_retry() == 0

    def test_invalid_max_retries_env_falls_back_to_3(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        monkeypatch.setenv("GITHUB_HTTP_MAX_RETRIES", "not-a-number")
        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.total == 3

    def test_invalid_backoff_env_falls_back_to_1_0(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        monkeypatch.setenv("GITHUB_HTTP_BACKOFF_FACTOR", "bad")
        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.backoff_factor == 1.0

    def test_respect_retry_after_header_is_false(self) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.respect_retry_after_header is False

    def test_raise_on_status_is_false(self) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.raise_on_status is False

    def test_429_with_retry_after_is_not_retried(self) -> None:
        """429 with Retry-After must NOT be retried at the transport layer.

        urllib3 v2 is_retry() retries 413/429/503 when they carry a
        Retry-After header and respect_retry_after_header=True, even if
        those codes are absent from status_forcelist.  We set
        respect_retry_after_header=False so 429s always surface as
        GithubException -> RateLimitException for the worker deferral path.
        """
        from dev_health_ops.providers.github.client import _github_http_retry

        retry = _github_http_retry()
        assert not isinstance(retry, int), "retry must be a Retry object for this test"
        assert retry.is_retry("GET", 429, has_retry_after=True) is False

    def test_503_by_status_is_retried(self) -> None:
        """503 in status_forcelist must be retried (no Retry-After needed)."""
        from dev_health_ops.providers.github.client import _github_http_retry

        retry = _github_http_retry()
        assert not isinstance(retry, int), "retry must be a Retry object for this test"
        assert retry.is_retry("GET", 503, has_retry_after=False) is True

    def test_backoff_max_default_is_30(self) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.backoff_max == 30.0

    def test_backoff_max_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        monkeypatch.setenv("GITHUB_HTTP_BACKOFF_MAX", "60")
        result = _github_http_retry()
        assert isinstance(result, Retry)
        assert result.backoff_max == 60.0

    def test_503_retried_up_to_total_then_exhausted(self) -> None:
        """Behavioral: urllib3 Retry.increment drives 503 retries up to total.

        No mock HTTP library is available (responses/requests_mock absent).
        We drive urllib3's Retry state machine directly via increment() to
        prove that a 503 is counted as a retry attempt and that the counter
        exhausts after `total` increments, matching the bounded-retry contract.
        """
        import urllib3.exceptions
        from urllib3.response import HTTPResponse
        from urllib3.util.retry import Retry

        from dev_health_ops.providers.github.client import _github_http_retry

        retry = _github_http_retry()
        assert isinstance(retry, Retry)
        total = retry.total
        assert isinstance(total, int)

        # Simulate `total` consecutive 503 responses via increment().
        # Each call returns a new Retry with a decremented counter.
        current = retry
        for _ in range(total):
            response = HTTPResponse(status=503)
            current = current.increment(method="GET", url="/", response=response)

        # After exhausting all retries the next increment must raise MaxRetryError.
        response = HTTPResponse(status=503)
        with pytest.raises(urllib3.exceptions.MaxRetryError):
            current.increment(method="GET", url="/", response=response)


class TestGithubHttpTimeout:
    def test_default_timeout_is_30(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from dev_health_ops.providers.github.client import _github_http_timeout

        monkeypatch.delenv("GITHUB_HTTP_TIMEOUT_SECONDS", raising=False)
        assert _github_http_timeout() == 30

    def test_env_override_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from dev_health_ops.providers.github.client import _github_http_timeout

        monkeypatch.setenv("GITHUB_HTTP_TIMEOUT_SECONDS", "60")
        assert _github_http_timeout() == 60

    def test_invalid_env_falls_back_to_30(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dev_health_ops.providers.github.client import _github_http_timeout

        monkeypatch.setenv("GITHUB_HTTP_TIMEOUT_SECONDS", "oops")
        assert _github_http_timeout() == 30

    def test_minimum_is_1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from dev_health_ops.providers.github.client import _github_http_timeout

        monkeypatch.setenv("GITHUB_HTTP_TIMEOUT_SECONDS", "0")
        assert _github_http_timeout() == 1

        monkeypatch.setenv("GITHUB_HTTP_TIMEOUT_SECONDS", "-5")
        assert _github_http_timeout() == 1


class TestGithubWorkClientUsesRetry:
    def test_github_constructed_with_non_none_retry(self) -> None:
        """GitHubWorkClient must pass a non-None retry to Github().

        Mirrors the patch pattern used in test_github_app_auth.py.
        """
        from urllib3.util.retry import Retry

        with (
            patch("github.Github") as github_cls,
            patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
        ):
            GitHubWorkClient(auth=GitHubAuth(token="fake"))

        github_cls.assert_called_once()
        call_kwargs = github_cls.call_args.kwargs
        retry_arg = call_kwargs.get("retry")
        assert retry_arg is not None, "Github() must be called with a non-None retry"
        assert isinstance(retry_arg, (Retry, int))

    def test_github_constructed_with_timeout(self) -> None:
        """GitHubWorkClient must pass a timeout to Github()."""
        with (
            patch("github.Github") as github_cls,
            patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
        ):
            GitHubWorkClient(auth=GitHubAuth(token="fake"))

        call_kwargs = github_cls.call_args.kwargs
        assert "timeout" in call_kwargs
        assert call_kwargs["timeout"] >= 1

    def test_github_constructed_with_base_url_uses_retry(self) -> None:
        """The base_url branch also passes retry and timeout."""
        from urllib3.util.retry import Retry

        with (
            patch("github.Github") as github_cls,
            patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
        ):
            GitHubWorkClient(
                auth=GitHubAuth(
                    token="fake", base_url="https://github.example.com/api/v3"
                )
            )

        github_cls.assert_called_once()
        call_kwargs = github_cls.call_args.kwargs
        retry_arg = call_kwargs.get("retry")
        assert retry_arg is not None
        assert isinstance(retry_arg, (Retry, int))
        assert "timeout" in call_kwargs
