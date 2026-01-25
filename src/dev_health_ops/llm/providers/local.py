"""
Local/OpenAI-compatible LLM provider.

Supports Ollama, LMStudio, vLLM, and other OpenAI-compatible endpoints.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from .openai import (
    OpenAIProviderConfig,
    OpenAIGPT5Provider,
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


class LocalProvider:
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

    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
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
        self.model = model or os.getenv("LOCAL_LLM_MODEL", "llama3.2")
        self.api_key = api_key or os.getenv("LOCAL_LLM_API_KEY", "not-needed")
        self.max_completion_tokens = max_completion_tokens
        self.temperature = temperature
        self._client: Optional[object] = None

    def _get_client(self) -> object:
        """Lazy initialize OpenAI client pointing to local server."""
        if self._client is None:
            try:
                from openai import AsyncOpenAI

                self._client = AsyncOpenAI(
                    api_key=self.api_key,
                    base_url=self.base_url,
                )
            except ImportError:
                raise ImportError(
                    "OpenAI package not installed. Install with: pip install openai"
                )
        return self._client

    async def complete(self, prompt: str) -> str:
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
        response_format: Optional[dict] = None
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
                return validate_json_or_empty(content) if is_schema_prompt else content

            except Exception as e:
                # If we get a 400 error, it's likely that the server doesn't support
                # the requested response_format (common with local OpenAI-compatible APIs).
                if "400" in str(e) and response_format and retry_count < max_retries:
                    logger.warning(
                        "Local LLM API error (likely unsupported response_format). Retrying with text format. Error: %s",
                        e,
                    )
                    # Fallback to plain text JSON request
                    response_format = {"type": "text"}
                    retry_count += 1
                    continue

                logger.error("Local LLM API error (%s): %s", self.base_url, e)
                raise
        return ""  # Should not be reachable

    async def aclose(self) -> None:
        if self._client:
            await self._client.close()  # type: ignore


class OllamaProvider(LocalProvider):
    """Ollama-specific provider with sensible defaults."""

    def __init__(
        self,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            base_url=base_url
            or os.getenv("OLLAMA_BASE_URL", DEFAULT_ENDPOINTS["ollama"]),
            model=model or os.getenv("OLLAMA_MODEL", "llama3.2"),
            **kwargs,
        )


class LMStudioProvider(LocalProvider):
    """LMStudio-specific provider with sensible defaults."""

    def __init__(
        self,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            base_url=base_url
            or os.getenv("LMSTUDIO_BASE_URL", DEFAULT_ENDPOINTS["lmstudio"]),
            # LMStudio typically serves whatever model is loaded
            model=model or os.getenv("LMSTUDIO_MODEL", "local-model"),
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
        base_url: Optional[str] = None,
        max_completion_tokens: int = 4096,
        temperature: float = 0.3,
    ) -> None:
        base_url = base_url or os.getenv(
            "LMSTUDIO_BASE_URL", DEFAULT_ENDPOINTS["lmstudio"]
        )
        # Use dummy API key for local
        cfg = OpenAIProviderConfig(
            api_key="lm-studio",
            base_url=base_url,
            model=model,
            max_output_tokens=max_completion_tokens,
            temperature=temperature,
        )
        super().__init__(cfg)
