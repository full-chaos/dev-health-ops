"""
Local/OpenAI-compatible LLM provider.

Supports Ollama, LMStudio, vLLM, and other OpenAI-compatible endpoints.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

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
        max_tokens: int = 1024,
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
        self.max_tokens = max_tokens
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

        try:
            response = await client.chat.completions.create(  # type: ignore
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an assistant that explains precomputed work analytics. "
                            "Use probabilistic language (appears, leans, suggests). "
                            "Never use definitive language (is, was, detected, determined)."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )

            return response.choices[0].message.content or ""

        except Exception as e:
            logger.error("Local LLM API error (%s): %s", self.base_url, e)
            raise


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
