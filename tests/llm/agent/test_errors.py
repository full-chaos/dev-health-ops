from __future__ import annotations

import pytest

from dev_health_ops.llm.agent.errors import (
    AgentProviderError,
    AgentProviderErrorCode,
    safe_agent_provider_error,
)


class _StatusError(RuntimeError):
    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


@pytest.mark.parametrize(
    ("raw", "code", "retryable"),
    [
        (
            _StatusError("invalid_api_key", 401),
            AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED,
            False,
        ),
        (
            _StatusError("model_not_found", 404),
            AgentProviderErrorCode.MODEL_NOT_SUPPORTED,
            False,
        ),
        (
            _StatusError("Unsupported parameter: temperature", 400),
            AgentProviderErrorCode.INVALID_REQUEST,
            False,
        ),
        (
            _StatusError("rate_limit_exceeded", 429),
            AgentProviderErrorCode.RATE_LIMITED,
            True,
        ),
        (TimeoutError(), AgentProviderErrorCode.TIMEOUT, True),
        (
            ConnectionError("connection refused"),
            AgentProviderErrorCode.PROVIDER_UNAVAILABLE,
            True,
        ),
    ],
)
def test_safe_agent_provider_error_preserves_safe_failure_category(
    raw: Exception, code: AgentProviderErrorCode, retryable: bool
) -> None:
    error = safe_agent_provider_error(raw)

    assert error.code is code
    assert error.retryable is retryable
    assert "temperature" not in str(error)


def test_output_exhausted_is_non_retryable_and_carries_no_provider_content() -> None:
    """CHAOS-3285: exhaustion is a structural, non-retryable model-capability
    mismatch (retrying the same request with the same budget cannot help),
    distinct from every other AgentProviderErrorCode member, none of which
    is OUTPUT_EXHAUSTED.
    """
    error = AgentProviderError(AgentProviderErrorCode.OUTPUT_EXHAUSTED)

    assert error.retryable is False
    assert error.code is AgentProviderErrorCode.OUTPUT_EXHAUSTED
    assert error.code not in {
        AgentProviderErrorCode.INVALID_RESPONSE,
        AgentProviderErrorCode.INVALID_REQUEST,
    }
