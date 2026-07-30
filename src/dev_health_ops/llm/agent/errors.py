"""Safe, stable error vocabulary for Ask Dev provider calls."""

from __future__ import annotations

from enum import Enum

from dev_health_ops.llm.errors import (
    LLMAuthError,
    LLMContextLengthError,
    LLMInvalidRequestError,
    LLMModelNotFoundError,
    LLMRateLimitError,
    LLMTimeoutError,
    LLMTransportError,
    classify_provider_error,
)


class AgentProviderErrorCode(str, Enum):
    DISABLED = "disabled"
    PROVIDER_NOT_CONFIGURED = "provider_not_configured"
    MODEL_NOT_SUPPORTED = "model_not_supported"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    INVALID_REQUEST = "invalid_request"
    RATE_LIMITED = "rate_limited"
    INVALID_RESPONSE = "invalid_response"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    BUDGET_EXHAUSTED = "budget_exhausted"
    BUDGET_UNAVAILABLE = "budget_unavailable"


_SAFE_MESSAGES = {
    AgentProviderErrorCode.DISABLED: "Ask Dev is disabled.",
    AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED: "No usable model provider is configured.",
    AgentProviderErrorCode.MODEL_NOT_SUPPORTED: "The selected model is not supported for Ask Dev.",
    AgentProviderErrorCode.PROVIDER_UNAVAILABLE: "The model provider is temporarily unavailable.",
    AgentProviderErrorCode.INVALID_REQUEST: "The model provider rejected this request.",
    AgentProviderErrorCode.RATE_LIMITED: "The model provider rate limit was reached.",
    AgentProviderErrorCode.INVALID_RESPONSE: "The model provider returned an invalid response.",
    AgentProviderErrorCode.TIMEOUT: "The model provider timed out.",
    AgentProviderErrorCode.CANCELLED: "The model request was cancelled.",
    AgentProviderErrorCode.BUDGET_EXHAUSTED: "The organization BYO LLM budget was reached.",
    AgentProviderErrorCode.BUDGET_UNAVAILABLE: "BYO LLM budget accounting is temporarily unavailable.",
}


class AgentProviderError(RuntimeError):
    def __init__(
        self,
        code: AgentProviderErrorCode,
        *,
        retryable: bool = False,
        provider_dispatched: bool = True,
    ):
        self.code = code
        self.retryable = retryable
        self.provider_dispatched = provider_dispatched
        super().__init__(_SAFE_MESSAGES[code])


def safe_agent_provider_error(exc: Exception) -> AgentProviderError:
    """Map provider failures without retaining provider text or credentials."""
    if isinstance(exc, AgentProviderError):
        return exc
    classified = classify_provider_error(exc)
    if isinstance(classified, LLMTimeoutError):
        return AgentProviderError(AgentProviderErrorCode.TIMEOUT, retryable=True)
    if isinstance(classified, LLMAuthError):
        return AgentProviderError(AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED)
    if isinstance(classified, LLMModelNotFoundError):
        return AgentProviderError(AgentProviderErrorCode.MODEL_NOT_SUPPORTED)
    if isinstance(classified, (LLMInvalidRequestError, LLMContextLengthError)):
        return AgentProviderError(AgentProviderErrorCode.INVALID_REQUEST)
    if isinstance(classified, LLMRateLimitError):
        return AgentProviderError(AgentProviderErrorCode.RATE_LIMITED, retryable=True)
    if isinstance(classified, LLMTransportError):
        return AgentProviderError(
            AgentProviderErrorCode.PROVIDER_UNAVAILABLE, retryable=True
        )
    return AgentProviderError(
        AgentProviderErrorCode.PROVIDER_UNAVAILABLE, retryable=True
    )
