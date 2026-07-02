"""Tests for the provider-neutral RateLimitSignal and the exception unification
that lets legacy connector rate limits reach the worker deferral path.

Covers CHAOS-2753:
  * RateLimitSignal construction + reset_at normalization (epoch-s / epoch-ms).
  * connectors.base.RateLimitException now subclasses the root exception.
  * GitHub primary/secondary/permission 403 triage populates signals at every
    classification site.
  * Linear complexity limit carries a signal but stays non-retryable.
  * reference_discovery backoff honors a server Retry-After.
  * LaunchDarkly 403 maps to AuthenticationException (not a retryable API error).
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import cast

import httpx
import pytest

from dev_health_ops.exceptions import (
    AuthenticationException,
)
from dev_health_ops.exceptions import (
    RateLimitException as RootRateLimitException,
)
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal

# A fixed, valid epoch (2023-11-14T22:13:20Z) reused across reset-window cases.
_EPOCH_S = 1_700_000_000
_EPOCH_UTC = datetime.fromtimestamp(_EPOCH_S, tz=timezone.utc)


class _FakeResponse:
    """Minimal stand-in for requests.Response used by the GitHub sites."""

    def __init__(
        self,
        status_code: int,
        *,
        headers: dict[str, str] | None = None,
        text: str = "",
        url: str = "https://api.github.com/graphql",
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self.url = url

    def json(self) -> dict[str, object]:  # pragma: no cover - not reached on 403
        return {}


# ---------------------------------------------------------------------------
# RateLimitSignal core
# ---------------------------------------------------------------------------


class TestRateLimitSignal:
    def test_dimension_reuses_budget_dimension(self) -> None:
        sig = RateLimitSignal(
            provider="github", dimension=BudgetDimension.SECONDARY_ABUSE_RISK
        )
        assert sig.dimension is BudgetDimension.SECONDARY_ABUSE_RISK
        # Worker-boundary fields stay None at the client (enriched in ws-d).
        assert sig.integration_id is None
        assert sig.route_family is None

    def test_to_dict_serializes_enum_and_datetime(self) -> None:
        sig = RateLimitSignal(
            provider="jira",
            host="example.atlassian.net",
            dimension=BudgetDimension.REST_CORE,
            retry_after_seconds=12.0,
            reset_at=_EPOCH_UTC,
            reason="primary",
            request_id="req-1",
        )
        d = sig.to_dict()
        assert d["provider"] == "jira"
        assert d["dimension"] == "rest_core"
        assert d["reset_at"] == _EPOCH_UTC.isoformat()
        assert d["reason"] == "primary"
        assert d["request_id"] == "req-1"


def test_signal_reset_at_normalized_to_utc() -> None:
    # GitHub / GitLab report epoch SECONDS.
    sig_seconds = RateLimitSignal(
        provider="github",
        reset_at=RateLimitSignal.reset_at_from_epoch_seconds(_EPOCH_S),
    )
    assert sig_seconds.reset_at == _EPOCH_UTC
    assert sig_seconds.reset_at is not None
    assert sig_seconds.reset_at.tzinfo is timezone.utc

    # Linear reports epoch MILLISECONDS -- the same instant.
    sig_millis = RateLimitSignal(
        provider="linear",
        reset_at=RateLimitSignal.reset_at_from_epoch_millis(_EPOCH_S * 1000),
    )
    assert sig_millis.reset_at == _EPOCH_UTC
    assert sig_seconds.reset_at == sig_millis.reset_at

    # String header values are coerced.
    assert RateLimitSignal.reset_at_from_epoch_seconds(str(_EPOCH_S)) == _EPOCH_UTC

    # Absent / zero reset windows (Linear's default header is 0) -> None.
    assert RateLimitSignal.reset_at_from_epoch_millis(0) is None
    assert RateLimitSignal.reset_at_from_epoch_seconds(None) is None
    assert RateLimitSignal.reset_at_from_epoch_seconds("bogus") is None

    # A tz-naive datetime is normalized to UTC.
    naive = RateLimitSignal(provider="x", reset_at=datetime(2026, 1, 1, 0, 0, 0))
    assert naive.reset_at is not None
    assert naive.reset_at.tzinfo is timezone.utc


def test_signal_reset_at_from_iso8601_for_jira() -> None:
    # Jira's X-RateLimit-Reset is an ISO 8601 timestamp (Atlassian Cloud
    # rate-limiting docs), not epoch seconds/millis like GitHub/GitLab/Linear
    # (CHAOS-2758 verification of the previously-unverified epoch unit).
    sig = RateLimitSignal(
        provider="jira",
        reset_at=RateLimitSignal.reset_at_from_iso8601("2025-10-08T15:00:00Z"),
    )
    assert sig.reset_at == datetime(2025, 10, 8, 15, 0, 0, tzinfo=timezone.utc)

    # An explicit offset is honored and normalized to UTC.
    offset = RateLimitSignal.reset_at_from_iso8601("2025-10-08T16:00:00+01:00")
    assert offset == datetime(2025, 10, 8, 15, 0, 0, tzinfo=timezone.utc)

    # Absent / malformed / wrong-type values degrade to None (Retry-After
    # stays authoritative regardless).
    assert RateLimitSignal.reset_at_from_iso8601(None) is None
    assert RateLimitSignal.reset_at_from_iso8601("") is None
    assert RateLimitSignal.reset_at_from_iso8601("not-a-timestamp") is None
    assert RateLimitSignal.reset_at_from_iso8601(1_700_000_000) is None


def test_legacy_connector_rate_limit_subclasses_root() -> None:
    """The class-split bug fix: base.RateLimitException IS-A root RateLimitException."""
    from dev_health_ops.connectors.base import (
        RateLimitException as LegacyRateLimitException,
    )

    assert issubclass(LegacyRateLimitException, RootRateLimitException)
    exc = LegacyRateLimitException("boom", retry_after_seconds=15.0)
    assert isinstance(exc, RootRateLimitException)
    assert exc.retry_after_seconds == 15.0
    # Inherits the root constructor (keyword-only signal, defaults to None).
    assert exc.signal is None


def test_handle_github_exception_forwards_signal() -> None:
    """Root->legacy normalization in _handle_github_exception must carry the
    signal, or the GraphQL classification sites populate signals that never
    reach the worker (found in PR #1111 review)."""
    from dev_health_ops.connectors.base import (
        RateLimitException as LegacyRateLimitException,
    )

    conn = _new_github_connector()
    signal = RateLimitSignal(
        provider="github",
        dimension=BudgetDimension.GRAPHQL_COST,
        reason="primary",
        retry_after_seconds=30.0,
    )
    root_exc = RootRateLimitException(
        "graphql limited", retry_after_seconds=30.0, signal=signal
    )
    with pytest.raises(LegacyRateLimitException) as exc_info:
        conn._handle_github_exception(root_exc)
    assert exc_info.value.retry_after_seconds == 30.0
    assert exc_info.value.signal is signal


# ---------------------------------------------------------------------------
# GitHub 403 triage populates signals at all three classification sites
# ---------------------------------------------------------------------------


def _new_github_connector():
    from dev_health_ops.connectors.github import GitHubConnector

    conn = GitHubConnector.__new__(GitHubConnector)
    # _rest_base_url() reads self.github; None -> default api.github.com.
    conn.github = None  # type: ignore[assignment]
    return conn


class TestGitHub403TriagePopulatesSignalReason:
    def test_rest_classify_site(self) -> None:
        conn = _new_github_connector()

        primary = conn._classify_github_403(
            _FakeResponse(
                403,
                headers={
                    "x-ratelimit-remaining": "0",
                    "x-ratelimit-reset": str(_EPOCH_S),
                    "x-github-request-id": "REQ-P",
                },
            ),
            "GET",
            "/repos/o/r/commits",
        )
        assert isinstance(primary, RootRateLimitException)
        assert primary.signal is not None
        assert primary.signal.reason == "primary"
        assert primary.signal.dimension is BudgetDimension.REST_CORE
        assert primary.signal.provider == "github"
        assert primary.signal.reset_at == _EPOCH_UTC
        assert primary.signal.request_id == "REQ-P"

        secondary = conn._classify_github_403(
            _FakeResponse(
                403, headers={"retry-after": "60"}, text="secondary rate limit"
            ),
            "GET",
            "/repos/o/r",
        )
        assert isinstance(secondary, RootRateLimitException)
        assert secondary.signal is not None
        assert secondary.signal.reason == "secondary"
        assert secondary.signal.dimension is BudgetDimension.SECONDARY_ABUSE_RISK
        assert secondary.signal.retry_after_seconds == 60.0

        # Permission/SSO 403 is not a rate limit -> no exception, no signal.
        permission = conn._classify_github_403(
            _FakeResponse(
                403, headers={}, text="Resource not accessible by integration"
            ),
            "GET",
            "/repos/o/r",
        )
        assert permission is None

    def test_pygithub_raise_site(self) -> None:
        conn = _new_github_connector()

        primary_exc = SimpleNamespace(
            headers={
                "x-ratelimit-remaining": "0",
                "x-ratelimit-reset": str(_EPOCH_S),
                "x-github-request-id": "REQ-P",
            },
            data={"message": "API rate limit exceeded"},
        )
        with pytest.raises(RootRateLimitException) as excinfo:
            conn._raise_github_403(primary_exc)
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "primary"
        assert excinfo.value.signal.dimension is BudgetDimension.REST_CORE
        assert excinfo.value.signal.reset_at == _EPOCH_UTC

        secondary_exc = SimpleNamespace(
            headers={"retry-after": "30"},
            data={"message": "secondary rate limit"},
        )
        with pytest.raises(RootRateLimitException) as excinfo:
            conn._raise_github_403(secondary_exc)
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "secondary"
        assert excinfo.value.signal.dimension is BudgetDimension.SECONDARY_ABUSE_RISK
        assert excinfo.value.signal.retry_after_seconds == 30.0

        permission_exc = SimpleNamespace(
            headers={}, data={"message": "SAML SSO required"}
        )
        with pytest.raises(AuthenticationException):
            conn._raise_github_403(permission_exc)

    def test_graphql_site(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from dev_health_ops.connectors.utils import graphql as graphql_mod

        client = graphql_mod.GitHubGraphQLClient(token="x")
        # Bypass the retry_with_backoff wrapper so the classification runs once.
        raw_query = graphql_mod.GitHubGraphQLClient.query.__wrapped__

        monkeypatch.setattr(
            graphql_mod.requests,
            "post",
            lambda *a, **k: _FakeResponse(
                403,
                headers={
                    "x-ratelimit-remaining": "0",
                    "x-ratelimit-reset": str(_EPOCH_S),
                    "x-github-request-id": "G-REQ",
                },
            ),
        )
        with pytest.raises(RootRateLimitException) as excinfo:
            raw_query(client, "{ viewer { login } }")
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "primary"
        assert excinfo.value.signal.dimension is BudgetDimension.GRAPHQL_COST
        assert excinfo.value.signal.host == "api.github.com"
        assert excinfo.value.signal.request_id == "G-REQ"

        monkeypatch.setattr(
            graphql_mod.requests,
            "post",
            lambda *a, **k: _FakeResponse(
                403,
                headers={"retry-after": "45"},
                text="You have exceeded a secondary rate limit",
            ),
        )
        with pytest.raises(RootRateLimitException) as excinfo:
            raw_query(client, "{ x }")
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "secondary"
        assert excinfo.value.signal.dimension is BudgetDimension.SECONDARY_ABUSE_RISK

        monkeypatch.setattr(
            graphql_mod.requests,
            "post",
            lambda *a, **k: _FakeResponse(
                403,
                headers={},
                text="Resource not accessible by personal access token",
            ),
        )
        with pytest.raises(AuthenticationException):
            raw_query(client, "{ x }")

    def test_pygithub_provider_client_site(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from github import GithubException

        from dev_health_ops.providers.github import client as gh_client_mod

        conn = gh_client_mod.GitHubWorkClient.__new__(gh_client_mod.GitHubWorkClient)
        monkeypatch.setattr(
            conn,
            "github",
            SimpleNamespace(rate_limiting_resettime=None),
            raising=False,
        )
        monkeypatch.setattr(
            conn, "_record_rest_usage", lambda *a, **k: None, raising=False
        )

        primary = GithubException(
            403,
            data={"message": "API rate limit exceeded"},
            headers={
                "x-ratelimit-remaining": "0",
                "x-ratelimit-reset": str(_EPOCH_S),
                "x-github-request-id": "P",
            },
        )
        with pytest.raises(RootRateLimitException) as excinfo:
            conn._raise_github_exception(primary, operation="list_pull_requests")
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "primary"
        assert excinfo.value.signal.dimension is BudgetDimension.REST_CORE
        assert excinfo.value.signal.reset_at == _EPOCH_UTC
        assert excinfo.value.signal.request_id == "P"

        secondary = GithubException(
            403,
            data={"message": "You have exceeded a secondary rate limit"},
            headers={"retry-after": "42"},
        )
        with pytest.raises(RootRateLimitException) as excinfo:
            conn._raise_github_exception(secondary, operation="list_pull_requests")
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "secondary"
        assert excinfo.value.signal.dimension is BudgetDimension.SECONDARY_ABUSE_RISK
        assert excinfo.value.signal.retry_after_seconds == 42.0

        permission = GithubException(
            403,
            data={"message": "Resource not accessible by integration"},
            headers={},
        )
        with pytest.raises(AuthenticationException):
            conn._raise_github_exception(permission, operation="list_pull_requests")


# ---------------------------------------------------------------------------
# Linear complexity limit: signal without turning it retryable
# ---------------------------------------------------------------------------


def test_linear_complexity_emits_signal_and_stays_non_retryable() -> None:
    from dev_health_ops.providers.linear.client import (
        LinearClient,
        LinearComplexityLimitError,
        LinearGraphQLError,
    )

    errors = [
        {"message": "Query too complex", "extensions": {"code": "COMPLEXITY_LIMIT"}}
    ]
    with pytest.raises(LinearComplexityLimitError) as excinfo:
        LinearClient._raise_graphql_errors(errors)

    exc = excinfo.value
    # Stays a GraphQL error, NOT a rate-limit error -> never re-driven as
    # deferrable/retryable work by the worker.
    assert isinstance(exc, LinearGraphQLError)
    assert not isinstance(exc, RootRateLimitException)
    assert exc.signal is not None
    assert exc.signal.provider == "linear"
    assert exc.signal.reason == "complexity"
    assert exc.signal.dimension is BudgetDimension.GRAPHQL_COST


# ---------------------------------------------------------------------------
# reference_discovery honors Retry-After
# ---------------------------------------------------------------------------


def test_reference_discovery_honors_retry_after() -> None:
    from dev_health_ops.workers.reference_discovery import (
        _reference_discovery_backoff_seconds,
    )

    # attempt 1 -> base 30s (+ up to 30s jitter), no server hint.
    attempt_only = _reference_discovery_backoff_seconds(1)
    assert 30 <= attempt_only <= 60

    # A server Retry-After larger than the attempt-based base wins.
    with_retry_after = _reference_discovery_backoff_seconds(1, 600.0)
    assert with_retry_after >= 600

    # Still capped at the 900s ceiling (+ jitter).
    capped = _reference_discovery_backoff_seconds(1, 100_000.0)
    assert 900 <= capped <= 930


# ---------------------------------------------------------------------------
# LaunchDarkly 403 -> AuthenticationException (both LD client modules)
# ---------------------------------------------------------------------------


def test_launchdarkly_403_is_authentication_error() -> None:
    from dev_health_ops.connectors.launchdarkly import (
        _raise_for_status as connector_raise,
    )
    from dev_health_ops.providers.launchdarkly.code_refs import (
        _raise_for_status as code_refs_raise,
    )

    forbidden = cast(
        httpx.Response,
        SimpleNamespace(status_code=403, text="forbidden", headers={}, url=None),
    )
    with pytest.raises(AuthenticationException):
        connector_raise(forbidden)
    with pytest.raises(AuthenticationException):
        code_refs_raise(forbidden)

    # 429 still yields a retryable RateLimitException carrying a signal.
    throttled = cast(
        httpx.Response,
        SimpleNamespace(
            status_code=429,
            text="",
            headers={"Retry-After": "7", "X-RateLimit-Reset": str(_EPOCH_S * 1000)},
            url=None,
        ),
    )
    with pytest.raises(RootRateLimitException) as excinfo:
        connector_raise(throttled)
    assert excinfo.value.signal is not None
    assert excinfo.value.signal.provider == "launchdarkly"
    assert excinfo.value.signal.reason == "primary"
    assert excinfo.value.signal.dimension is BudgetDimension.REST_CORE
    assert excinfo.value.signal.retry_after_seconds == 7.0
    assert excinfo.value.signal.reset_at == _EPOCH_UTC
