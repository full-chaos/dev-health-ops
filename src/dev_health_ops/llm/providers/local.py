"""
Local/OpenAI-compatible LLM provider.

Supports Ollama, LMStudio, vLLM, and other OpenAI-compatible endpoints.
"""

from __future__ import annotations

import asyncio
import logging
import os
from urllib.parse import urlsplit, urlunsplit

from dev_health_ops.llm.errors import (
    LLMRateLimitError,
    classify_provider_error,
    is_retryable,
    retry_delay,
)

from ._http import make_hardened_async_httpx_client
from .base import (
    DEFAULT_MODEL_BY_PROVIDER,
    CompletionResult,
    LLMProviderBase,
    usage_token_count,
)
from .openai import (
    OpenAIGPT5Provider,
    OpenAIProviderConfig,
    categorization_json_schema,
    is_json_schema_prompt,
    system_message,
    validate_json_or_empty,
)

logger = logging.getLogger(__name__)

# Default endpoints for common local providers
DEFAULT_ENDPOINTS = {
    "ollama": "http://localhost:11434/v1",
    "lmstudio": "http://localhost:1234/v1",
    "vllm": "http://localhost:8000/v1",
    "local": "http://localhost:11434/v1",  # Default to Ollama
}


def _redact_url(url: str) -> str:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return "[invalid-url]"
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _status_code(exc: BaseException) -> int | None:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return status_code
    response = getattr(exc, "response", None)
    response_status = getattr(response, "status_code", None)
    return response_status if isinstance(response_status, int) else None


def _error_metadata(exc: BaseException) -> dict[str, str | int | None]:
    return {"type": type(exc).__name__, "status_code": _status_code(exc)}


def _first_env(names: tuple[str, ...]) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return ""


def _env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _optional_env_flag(name: str) -> bool | None:
    value = os.getenv(name)
    if value is None:
        return None
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _lmstudio_validate_model_on_startup() -> bool:
    provider_flag = _optional_env_flag("LMSTUDIO_VALIDATE_MODEL_ON_STARTUP")
    if provider_flag is not None:
        return provider_flag
    return _env_flag("LLM_VALIDATE_MODEL_ON_STARTUP")


class LocalProvider(LLMProviderBase):
    """
    OpenAI-compatible provider for local LLM servers.

    Supports:
    - Ollama (default: http://localhost:11434/v1)
    - LMStudio (default: http://localhost:1234/v1)
    - vLLM (default: http://localhost:8000/v1)
    - Any OpenAI-compatible endpoint

    Configure via environment variables:
    - LOCAL_LLM_BASE_URL: Custom endpoint URL
    - LOCAL_LLM_MODEL: Model name (default: varies by provider)
    - LOCAL_LLM_API_KEY: API key if required (default: "not-needed")
    """

    provider_name = "local"

    def __init__(
        self,
        base_url: str | None = None,
        model: str | None = None,
        api_key: str | None = None,
        max_completion_tokens: int = 4096,
        temperature: float = 0.3,
    ) -> None:
        """
        Initialize local provider.

        Args:
            base_url: OpenAI-compatible API base URL
            model: Model name to use
            api_key: API key (some local servers don't need one)
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature
        """
        self.base_url = base_url or os.getenv(
            "LOCAL_LLM_BASE_URL", DEFAULT_ENDPOINTS["local"]
        )
        self.model: str = (
            model or os.getenv("LOCAL_LLM_MODEL") or DEFAULT_MODEL_BY_PROVIDER["local"]
        )
        self.api_key = api_key or os.getenv("LOCAL_LLM_API_KEY", "not-needed")
        self.max_completion_tokens = max_completion_tokens
        self.temperature = temperature
        self._client: object | None = None

    def _get_client(self) -> object:
        """Lazy initialize OpenAI client pointing to local server."""
        if self._client is None:
            try:
                from openai import AsyncOpenAI

                self._client = AsyncOpenAI(
                    api_key=self.api_key,
                    base_url=self.base_url,
                    http_client=make_hardened_async_httpx_client(),
                    max_retries=0,
                )
            except ImportError:
                raise ImportError(
                    "OpenAI package not installed. Install with: pip install openai"
                )
        return self._client

    async def complete(self, prompt: str) -> CompletionResult:
        """
        Generate a completion using the local LLM server.

        Args:
            prompt: The prompt text to complete

        Returns:
            The generated completion text
        """
        client = self._get_client()

        # Retry once on 400 errors (which often indicate unsupported response_format)
        retry_count = 0
        max_retries = 1

        is_schema_prompt = is_json_schema_prompt(prompt)
        sys_msg = system_message(prompt)

        # Start with a modern response_format if it's a JSON prompt
        response_format: dict | None = None
        if is_schema_prompt:
            # Try Structured Outputs if the server supports it
            response_format = {
                "type": "json_schema",
                "json_schema": {
                    "name": "categorization",
                    "schema": categorization_json_schema(),
                    "strict": True,
                },
            }

        while retry_count <= max_retries:
            try:
                payload: dict = {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": sys_msg},
                        {"role": "user", "content": prompt},
                    ],
                    "max_completion_tokens": self.max_completion_tokens,
                    "temperature": self.temperature,
                }

                if response_format:
                    payload["response_format"] = response_format

                response = await client.chat.completions.create(**payload)  # type: ignore

                content = response.choices[0].message.content or ""
                usage = getattr(response, "usage", None)
                text = validate_json_or_empty(content) if is_schema_prompt else content
                return CompletionResult(
                    text=text,
                    input_tokens=usage_token_count(
                        usage, "prompt_tokens", "input_tokens"
                    ),
                    output_tokens=usage_token_count(
                        usage, "completion_tokens", "output_tokens"
                    ),
                    model=self.model,
                )

            except Exception as e:
                # If we get a 400 error, it's likely that the server doesn't support
                # the requested response_format (common with local OpenAI-compatible APIs).
                if "400" in str(e) and response_format and retry_count < max_retries:
                    logger.warning(
                        "Local LLM API rejected response_format; retrying with text format: %s",
                        _error_metadata(e),
                    )
                    # Fallback to plain text JSON request
                    response_format = {"type": "text"}
                    retry_count += 1
                    continue

                model_name = self.model or "local-model"
                llm_exc = classify_provider_error(
                    e, provider=self.provider_name, model=model_name
                )
                if is_retryable(llm_exc) and retry_count < max_retries:
                    delay = retry_delay(retry_count)
                    if isinstance(llm_exc, LLMRateLimitError) and llm_exc.retry_after:
                        delay = llm_exc.retry_after
                    logger.warning(
                        "%s LLM API transient error on attempt %d/%d; retrying in %.1fs: %s",
                        self.provider_name,
                        retry_count + 1,
                        max_retries + 1,
                        delay,
                        llm_exc,
                    )
                    retry_count += 1
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    "Local LLM API error url=%s error=%s classified=%s",
                    _redact_url(self.base_url or ""),
                    _error_metadata(e),
                    type(llm_exc).__name__,
                )
                raise llm_exc from e
        return CompletionResult(
            text="", input_tokens=None, output_tokens=None, model=self.model
        )

    async def aclose(self) -> None:
        if self._client:
            await self._client.close()  # type: ignore


class OllamaProvider(LocalProvider):
    """Ollama-specific provider with sensible defaults."""

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            base_url=base_url
            or os.getenv("OLLAMA_BASE_URL", DEFAULT_ENDPOINTS["ollama"]),
            model=model
            or os.getenv("OLLAMA_MODEL", DEFAULT_MODEL_BY_PROVIDER["ollama"]),
            **kwargs,
        )


class LMStudioProvider(LocalProvider):
    """LMStudio-specific provider with sensible defaults."""

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            base_url=base_url
            or os.getenv("LMSTUDIO_BASE_URL", DEFAULT_ENDPOINTS["lmstudio"]),
            # LMStudio typically serves whatever model is loaded
            model=model
            or os.getenv("LMSTUDIO_MODEL", DEFAULT_MODEL_BY_PROVIDER["lmstudio"]),
            **kwargs,
        )


class LMStudioGPT5Provider(OpenAIGPT5Provider):
    """
    LMStudio provider for openai/gpt-oss* models using the Responses API.
    These models require the new /v1/responses endpoint schema.
    """

    def __init__(
        self,
        model: str,
        base_url: str | None = None,
        api_key: str | None = None,
        max_completion_tokens: int = 4096,
        temperature: float = 0.3,
        validate_model_on_startup: bool | None = None,
    ) -> None:
        base_url = base_url or os.getenv(
            "LMSTUDIO_BASE_URL", DEFAULT_ENDPOINTS["lmstudio"]
        )
        api_key = api_key or _first_env(
            ("LMSTUDIO_API_KEY", "LLM_API_KEY", "LOCAL_LLM_API_KEY")
        )
        should_validate_model = (
            _lmstudio_validate_model_on_startup()
            if validate_model_on_startup is None
            else validate_model_on_startup
        )
        cfg = OpenAIProviderConfig(
            api_key=api_key or "lm-studio",
            base_url=base_url,
            model=model,
            max_output_tokens=max_completion_tokens,
            temperature=temperature,
            validate_model_on_startup=should_validate_model,
            validation_provider_name="lmstudio",
        )
        super().__init__(cfg)
