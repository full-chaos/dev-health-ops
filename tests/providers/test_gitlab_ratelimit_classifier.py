"""Tests for providers/gitlab/ratelimit.py (CHAOS-2773 CS1).

``providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit`` now delegates
to ``classify_gitlab_status`` / ``maybe_raise_gitlab_rate_limit`` here, which
are themselves built directly on the shared ``providers/_ratelimit.py``
primitives (``gitlab_403_is_rate_limited`` / ``gitlab_resolve_retry_after_seconds``,
#1142) -- so exactly one predicate/delay implementation exists for GitLab.
These tests pin behavior parity with the pre-extraction inline logic (already
covered end-to-end by ``tests/providers/test_gitlab_client_ratelimit.py``,
which stays green untouched) and additionally exercise the classifier
directly, independent of a python-gitlab exception object.
"""

from __future__ import annotations

import time

import pytest

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.providers.gitlab.ratelimit import (
    classify_gitlab_status,
    maybe_raise_gitlab_rate_limit,
)
from dev_health_ops.sync.budget_types import BudgetDimension


class TestClassifyGitlabStatus:
    def test_429_is_always_rate_limited_primary(self) -> None:
        result = classify_gitlab_status(status=429, headers={})
        assert result.is_rate_limit is True
        assert result.reason == "primary"

    def test_plain_403_no_headers_is_not_rate_limited(self) -> None:
        result = classify_gitlab_status(status=403, headers={})
        assert result.is_rate_limit is False
        assert result.reason is None

    def test_403_with_retry_after_is_rate_limited_secondary(self) -> None:
        result = classify_gitlab_status(status=403, headers={"Retry-After": "9"})
        assert result.is_rate_limit is True
        assert result.reason == "secondary"
        assert result.retry_after_seconds == pytest.approx(9.0)

    def test_403_with_rate_limit_remaining_zero_is_rate_limited_secondary(self) -> None:
        result = classify_gitlab_status(
            status=403, headers={"RateLimit-Remaining": "0"}
        )
        assert result.is_rate_limit is True
        assert result.reason == "secondary"

    def test_403_with_nonzero_remaining_and_no_retry_after_is_not_rate_limited(
        self,
    ) -> None:
        result = classify_gitlab_status(
            status=403, headers={"RateLimit-Remaining": "42"}
        )
        assert result.is_rate_limit is False

    def test_403_retry_after_falls_back_to_rate_limit_reset(self) -> None:
        reset_epoch = int(time.time()) + 300
        result = classify_gitlab_status(
            status=403,
            headers={"RateLimit-Remaining": "0", "RateLimit-Reset": str(reset_epoch)},
        )
        assert result.retry_after_seconds is not None
        assert 290 <= result.retry_after_seconds <= 300

    def test_401_is_not_rate_limited(self) -> None:
        assert classify_gitlab_status(status=401, headers={}).is_rate_limit is False

    def test_none_status_is_not_rate_limited(self) -> None:
        assert classify_gitlab_status(status=None, headers={}).is_rate_limit is False

    def test_none_headers_does_not_raise(self) -> None:
        assert classify_gitlab_status(status=403, headers=None).is_rate_limit is False
        result = classify_gitlab_status(status=429, headers=None)
        assert result.is_rate_limit is True
        assert result.retry_after_seconds is None


class TestMaybeRaiseGitlabRateLimit:
    def test_raises_with_signal_on_429(self) -> None:
        with pytest.raises(RateLimitException) as excinfo:
            maybe_raise_gitlab_rate_limit(
                status=429, headers={"Retry-After": "5"}, request_id="req-1"
            )
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.provider == "gitlab"
        assert signal.dimension is BudgetDimension.REST_CORE
        assert signal.reason == "primary"
        assert signal.request_id == "req-1"
        assert excinfo.value.retry_after_seconds == pytest.approx(5.0)

    def test_no_op_when_not_rate_limited(self) -> None:
        # Must return without raising -- callers rely on this to fall through
        # to their own non-rate-limit handling.
        maybe_raise_gitlab_rate_limit(status=403, headers={})
        maybe_raise_gitlab_rate_limit(status=200, headers={})


# ---------------------------------------------------------------------------
# providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit delegation
# ---------------------------------------------------------------------------


class TestClientDelegation:
    def test_gitlab_error_429_raises_with_cause_chained(self) -> None:
        import gitlab

        from dev_health_ops.providers.gitlab.client import (
            _maybe_raise_gitlab_rate_limit,
        )

        original = gitlab.exceptions.GitlabError("rate limited", response_code=429)
        with pytest.raises(RateLimitException) as excinfo:
            _maybe_raise_gitlab_rate_limit(original)
        assert excinfo.value.__cause__ is original

    def test_gitlab_error_plain_403_returns_none(self) -> None:
        import gitlab

        from dev_health_ops.providers.gitlab.client import (
            _maybe_raise_gitlab_rate_limit,
        )

        original = gitlab.exceptions.GitlabError("forbidden", response_code=403)
        _maybe_raise_gitlab_rate_limit(original)  # must not raise

    def test_non_gitlab_error_returns_none(self) -> None:
        from dev_health_ops.providers.gitlab.client import (
            _maybe_raise_gitlab_rate_limit,
        )

        _maybe_raise_gitlab_rate_limit(RuntimeError("boom"))  # must not raise

    def test_already_canonical_rate_limit_exception_reraised_as_is(self) -> None:
        from dev_health_ops.providers.gitlab.client import (
            _maybe_raise_gitlab_rate_limit,
        )

        original = RateLimitException("already classified")
        with pytest.raises(RateLimitException) as excinfo:
            _maybe_raise_gitlab_rate_limit(original)
        assert excinfo.value is original
