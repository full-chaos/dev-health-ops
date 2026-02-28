"""Unified LLM error types and retry utilities.

All provider implementations (openai, anthropic, gemini, local/ollama) raise
from this module so that callers can catch a single exception hierarchy instead
of importing provider-specific exception classes.

Error hierarchy
---------------
LLMError (base)
├── LLMAuthError          — bad/missing API key
├── LLMRateLimitError     — 429 / quota exceeded
├── LLMContextLengthError — prompt exceeds model context window
├── LLMServerError        — 5xx from provider
└── LLMOutputError        — empty or invalid output from model

Retry utilities
---------------
is_retryable(exc)    — True for transient errors worth retrying.
retry_delay(attempt) — Exponential-backoff delay (seconds) for attempt N.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Type

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------


class LLMError(Exception):
    """Base class for all LLM provider errors."""

    def __init__(
        self,
        message: str,
        *,
        provider: str = "",
        model: str = "",
        original: Optional[BaseException] = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.model = model
        self.original = original

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.provider:
            parts.append(f"provider={self.provider}")
        if self.model:
            parts.append(f"model={self.model}")
        if self.original:
            parts.append(f"cause={self.original!r}")
        return " | ".join(parts)


class LLMAuthError(LLMError):
    """Invalid or missing API key / credentials."""


class LLMRateLimitError(LLMError):
    """Rate limit or quota exceeded (HTTP 429)."""

    def __init__(
        self,
        message: str,
        *,
        retry_after: Optional[float] = None,
        **kwargs,
    ) -> None:
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class LLMContextLengthError(LLMError):
    """Prompt exceeds model context window."""


class LLMServerError(LLMError):
    """Transient server-side error (HTTP 5xx)."""


class LLMOutputError(LLMError):
    """Model returned empty or unparseable output."""


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------

_RETRYABLE_TYPES: tuple[Type[LLMError], ...] = (
    LLMRateLimitError,
    LLMServerError,
    LLMOutputError,
)


def is_retryable(exc: BaseException) -> bool:
    """Return True if *exc* is a transient error that warrants a retry."""
    return isinstance(exc, _RETRYABLE_TYPES)


def classify_provider_error(
    exc: BaseException,
    *,
    provider: str = "",
    model: str = "",
) -> LLMError:
    """Wrap a raw provider exception in the canonical LLMError hierarchy.

    Inspects the exception message/type for well-known patterns from the
    OpenAI, Anthropic, and local-server SDKs.

    Args:
        exc: The original exception raised by the provider SDK.
        provider: Provider name for context (e.g. "openai").
        model: Model name for context (e.g. "gpt-5-mini").

    Returns:
        An appropriate LLMError subclass wrapping *exc*.
    """
    msg = str(exc)
    msg_lower = msg.lower()

    # Auth errors
    if any(k in msg_lower for k in ("401", "invalid_api_key", "authentication", "unauthorized")):
        return LLMAuthError(msg, provider=provider, model=model, original=exc)

    # Rate limit errors
    if any(k in msg_lower for k in ("429", "rate_limit", "rate limit", "quota", "too many requests")):
        retry_after: Optional[float] = None
        # Some SDKs expose retry_after on the exception object
        if hasattr(exc, "retry_after"):
            try:
                retry_after = float(getattr(exc, "retry_after"))
            except (TypeError, ValueError):
                pass  # retry_after is not a valid number; leave as None
        return LLMRateLimitError(
            msg, retry_after=retry_after, provider=provider, model=model, original=exc
        )

    # Context length errors
    if any(
        k in msg_lower
        for k in (
            "context_length_exceeded",
            "maximum context length",
            "too many tokens",
            "input too long",
            "reduce your prompt",
        )
    ):
        return LLMContextLengthError(msg, provider=provider, model=model, original=exc)

    # Server errors
    if any(k in msg_lower for k in ("500", "502", "503", "504", "server error", "internal error")):
        return LLMServerError(msg, provider=provider, model=model, original=exc)

    # Fallback: treat as generic LLM error (non-retryable)
    return LLMError(msg, provider=provider, model=model, original=exc)


# ---------------------------------------------------------------------------
# Retry utilities
# ---------------------------------------------------------------------------

_BASE_DELAY_SECONDS = 0.5
_MAX_DELAY_SECONDS = 30.0


def retry_delay(attempt: int) -> float:
    """Exponential backoff delay for attempt N (0-indexed).

    Returns:
        Delay in seconds, capped at MAX_DELAY_SECONDS.
    """
    delay = _BASE_DELAY_SECONDS * (2**attempt)
    return min(delay, _MAX_DELAY_SECONDS)


async def call_with_retry(
    provider_name: str,
    model: str,
    call,
    max_retries: int = 1,
) -> str:
    """Execute an async LLM call with uniform retry / error handling.

    Args:
        provider_name: Human-readable provider name for logging.
        model: Model name for logging.
        call: Zero-argument async callable that returns a string.
        max_retries: Maximum number of retry attempts (default 1 per contract).

    Returns:
        The LLM output string. Empty string signals a non-fatal output error.

    Raises:
        LLMError: On non-retryable errors or after exhausting retries.
    """
    attempt = 0
    last_exc: Optional[BaseException] = None

    while attempt <= max_retries:
        try:
            return await call()
        except LLMError as exc:
            last_exc = exc
            if is_retryable(exc) and attempt < max_retries:
                delay = (
                    exc.retry_after  # type: ignore[attr-defined]
                    if isinstance(exc, LLMRateLimitError) and exc.retry_after
                    else retry_delay(attempt)
                )
                logger.warning(
                    "LLM %s error on attempt %d/%d; retrying in %.1fs — %r",
                    provider_name,
                    attempt + 1,
                    max_retries + 1,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue
            # Non-retryable or exhausted
            logger.error(
                "LLM %s/%s failed after %d attempt(s): %r",
                provider_name,
                model,
                attempt + 1,
                exc,
            )
            raise
        except Exception as raw_exc:
            llm_exc = classify_provider_error(
                raw_exc, provider=provider_name, model=model
            )
            last_exc = llm_exc
            if is_retryable(llm_exc) and attempt < max_retries:
                delay = retry_delay(attempt)
                logger.warning(
                    "LLM %s transient error on attempt %d/%d; retrying in %.1fs — %r",
                    provider_name,
                    attempt + 1,
                    max_retries + 1,
                    delay,
                    llm_exc,
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue
            logger.error(
                "LLM %s/%s failed after %d attempt(s): %r",
                provider_name,
                model,
                attempt + 1,
                llm_exc,
            )
            raise llm_exc from raw_exc

    # Should not be reached
    if last_exc:
        raise last_exc
    return ""
