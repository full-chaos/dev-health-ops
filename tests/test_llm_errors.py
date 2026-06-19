from __future__ import annotations

from dev_health_ops.llm.errors import (
    LLMAuthError,
    LLMError,
    LLMRateLimitError,
    classify_provider_error,
    is_retryable,
)


class SecretException(Exception):
    def __repr__(self) -> str:
        return "SecretException(api_key='sk-secret-value')"


class FakeRateLimitError(Exception):
    status_code = 429
    headers = {"Retry-After": "2.5"}


def test_llm_error_string_omits_original_repr() -> None:
    err = LLMError(
        "clean failure",
        provider="openai",
        model="gpt-5-mini",
        original=SecretException(),
    )

    rendered = str(err)

    assert "sk-secret-value" not in rendered
    assert "cause_type=SecretException" in rendered
    assert "provider=openai" in rendered
    assert "model=gpt-5-mini" in rendered


def test_llm_error_message_redacts_keys() -> None:
    err = LLMError("Authorization: Bearer sk-secret-value-1234567890")

    assert "sk-secret-value" not in str(err)
    assert "<redacted>" in str(err)


def test_insufficient_quota_is_deterministic_auth_error() -> None:
    raw = RuntimeError("Error code: insufficient_quota; api_key=sk-secret-value")

    err = classify_provider_error(raw, provider="openai", model="gpt-5-mini")

    assert isinstance(err, LLMAuthError)
    assert not is_retryable(err)
    assert "sk-secret-value" not in str(err)
    assert "quota exhausted" in str(err)


def test_rate_limit_exceeded_is_retryable_with_retry_after() -> None:
    raw = FakeRateLimitError("rate_limit_exceeded")

    err = classify_provider_error(raw, provider="openai", model="gpt-5-mini")

    assert isinstance(err, LLMRateLimitError)
    assert is_retryable(err)
    assert err.retry_after == 2.5


def test_model_not_found_names_provider_default() -> None:
    err = classify_provider_error(
        RuntimeError("model_not_found"), provider="openai", model="bogus-model"
    )

    assert type(err) is LLMError
    assert not is_retryable(err)
    assert "bogus-model" in str(err)
    assert "gpt-5-mini" in str(err)


class HugeNumericRetryAfterError(Exception):
    status_code = 429
    headers = {"Retry-After": "86400"}


class FarFutureDateRetryAfterError(Exception):
    status_code = 429
    headers = {"Retry-After": "Wed, 21 Oct 2099 07:28:00 GMT"}


def test_huge_numeric_retry_after_is_clamped() -> None:
    err = classify_provider_error(
        HugeNumericRetryAfterError("rate_limit_exceeded"),
        provider="openai",
        model="gpt-5-mini",
    )

    assert isinstance(err, LLMRateLimitError)
    # A day-long Retry-After must be clamped, not honored verbatim.
    assert err.retry_after == 60.0


def test_far_future_http_date_retry_after_is_clamped() -> None:
    err = classify_provider_error(
        FarFutureDateRetryAfterError("rate_limit_exceeded"),
        provider="openai",
        model="gpt-5-mini",
    )

    assert isinstance(err, LLMRateLimitError)
    assert err.retry_after == 60.0
