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
import re
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import TypeVar

logger = logging.getLogger(__name__)
T = TypeVar("T")

_SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"sk-[A-Za-z0-9_-]{8,}"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(
        r"(?i)((?:api[_-]?key|authorization|x-api-key|token)\s*[:=]\s*)"
        r"['\"]?[^'\"\s,;}]+"
    ),
)


def _sanitize_message(message: str) -> str:
    text = str(message or "LLM provider error")
    text = text.replace("\n", " ").replace("\r", " ").strip()
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(
            lambda m: f"{m.group(1)}<redacted>" if m.groups() else "<redacted>", text
        )
    return text[:500]


def _exception_text(exc: BaseException) -> str:
    fragments: list[str] = [str(exc)]
    for attr_name in ("code", "type", "status_code", "message"):
        value = getattr(exc, attr_name, None)
        if value is not None:
            fragments.append(str(value))
    body = getattr(exc, "body", None)
    if body is not None:
        fragments.append(str(body))
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            fragments.append(str(status_code))
    return " ".join(fragments)


def _status_code(exc: BaseException) -> int | None:
    for source in (exc, getattr(exc, "response", None)):
        if source is None:
            continue
        raw = getattr(source, "status_code", None)
        if raw is None:
            continue
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None


def _header_value(exc: BaseException, name: str) -> str | None:
    headers = getattr(exc, "headers", None)
    if headers is None and getattr(exc, "response", None) is not None:
        headers = getattr(getattr(exc, "response"), "headers", None)
    if not headers:
        return None
    for key in (name, name.lower(), name.title()):
        try:
            value = headers.get(key)
        except AttributeError:
            value = None
        if value:
            return str(value)
    return None


def _retry_after_seconds(exc: BaseException) -> float | None:
    raw = getattr(exc, "retry_after", None) or _header_value(exc, "Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        pass
    try:
        parsed = parsedate_to_datetime(str(raw))
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())


def _default_model_for_provider(provider: str) -> str | None:
    try:
        from dev_health_ops.llm.providers.base import DEFAULT_MODEL_BY_PROVIDER
    except Exception:
        return None
    return DEFAULT_MODEL_BY_PROVIDER.get(provider)


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
        original: BaseException | None = None,
    ) -> None:
        super().__init__(_sanitize_message(message))
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
            parts.append(f"cause_type={type(self.original).__name__}")
        return " | ".join(parts)


class LLMAuthError(LLMError):
    """Invalid or missing API key / credentials."""


class LLMRateLimitError(LLMError):
    """Rate limit or quota exceeded (HTTP 429)."""

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None = None,
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

_RETRYABLE_TYPES: tuple[type[LLMError], ...] = (
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
    msg = _exception_text(exc)
    msg_lower = msg.lower()
    status_code = _status_code(exc)

    if any(k in msg_lower for k in ("insufficient_quota", "current quota")):
        return LLMAuthError(
            "LLM quota exhausted. Check provider billing/quota or use a different API key.",
            provider=provider,
            model=model,
            original=exc,
        )

    if any(k in msg_lower for k in ("model_not_found", "model not found")):
        default_model = _default_model_for_provider(provider)
        hint = f" Provider default is '{default_model}'." if default_model else ""
        configured = f" configured model '{model}'" if model else " configured model"
        return LLMError(
            f"LLM model not found for provider '{provider}' using{configured}.{hint} Check --model or provider defaults.",
            provider=provider,
            model=model,
            original=exc,
        )

    # Auth errors
    if any(
        k in msg_lower
        for k in ("401", "invalid_api_key", "authentication", "unauthorized")
    ) or any(
        k in msg_lower
        for k in ("missing api key", "api key missing", "api_key is required")
    ):
        return LLMAuthError(
            "Invalid or missing LLM API key.",
            provider=provider,
            model=model,
            original=exc,
        )

    # Rate limit errors
    if (
        any(
            k in msg_lower
            for k in ("429", "rate_limit", "rate limit", "too many requests")
        )
        or status_code == 429
    ):
        retry_after = _retry_after_seconds(exc)
        return LLMRateLimitError(
            "Transient LLM rate limit from provider.",
            retry_after=retry_after,
            provider=provider,
            model=model,
            original=exc,
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
        return LLMContextLengthError(
            "LLM prompt exceeds the model context window.",
            provider=provider,
            model=model,
            original=exc,
        )

    # Server errors
    if any(
        k in msg_lower
        for k in ("500", "502", "503", "504", "server error", "internal error")
    ) or (status_code is not None and status_code >= 500):
        return LLMServerError(
            "Transient LLM provider server error.",
            provider=provider,
            model=model,
            original=exc,
        )

    if any(k in msg_lower for k in ("timeout", "timed out", "connection error")):
        return LLMServerError(
            "Transient LLM provider transport error.",
            provider=provider,
            model=model,
            original=exc,
        )

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
    call: Callable[[], Awaitable[T]],
    max_retries: int = 1,
) -> T:
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
    # Sanitize external inputs to prevent log injection (CWE-117)
    provider_name = provider_name.replace("\n", "").replace("\r", "")
    model = model.replace("\n", "").replace("\r", "")

    attempt = 0
    last_exc: BaseException | None = None

    while attempt <= max_retries:
        try:
            return await call()
        except LLMError as exc:
            last_exc = exc
            if is_retryable(exc) and attempt < max_retries:
                delay = retry_delay(attempt)
                if isinstance(exc, LLMRateLimitError) and exc.retry_after:
                    delay = exc.retry_after
                logger.warning(
                    "LLM %s error on attempt %d/%d; retrying in %.1fs — %s",
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
                "LLM %s/%s failed after %d attempt(s): %s",
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
                if isinstance(llm_exc, LLMRateLimitError) and llm_exc.retry_after:
                    delay = llm_exc.retry_after
                logger.warning(
                    "LLM %s transient error on attempt %d/%d; retrying in %.1fs — %s",
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
                "LLM %s/%s failed after %d attempt(s): %s",
                provider_name,
                model,
                attempt + 1,
                llm_exc,
            )
            raise llm_exc from raw_exc

    # Should not be reached
    if last_exc:
        raise last_exc
    raise RuntimeError("LLM retry loop exited without a result or exception")
