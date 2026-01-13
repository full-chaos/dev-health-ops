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
        max_completion_tokens: int = 1024,
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
        # ``max_completion_tokens`` is the name used by OpenAI for the
        # maximum length of a chat completion.  Historically this repo used
        # ``max_tokens`` which is ignored by the OpenAI API and caused
        # responses to be truncated or omitted.  Using the correct keyword
        # guarantees we never hit the undocumented length limit and keeps
        # the intent of the config clear.
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

        try:
            is_json_prompt = (
                "Output schema" in (prompt or "")
                and '"subcategories"' in (prompt or "")
                and '"evidence_quotes"' in (prompt or "")
                and '"uncertainty"' in (prompt or "")
            )
            system_message = (
                "You are a JSON generator. Return a single JSON object only. "
                "Do not output markdown, code fences, comments, or extra text."
                if is_json_prompt
                else (
                    "You are an assistant that explains precomputed work analytics. "
                    "Use probabilistic language (appears, leans, suggests). "
                    "Never use definitive language (is, was, detected, determined)."
                )
            )
            # Build the request payload.  When the prompt contains a JSON
            # schema we enable OpenAI's strict JSON mode.  The schema is
            # embedded in the prompt by callers via ``build_explanation_prompt``.
            payload: dict = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt},
                ],
                "max_completion_tokens": self.max_completion_tokens,
                "temperature": self.temperature,
            }

            if is_json_prompt:
                payload["response_format"] = {"type": "json_object"}

            response = await client.chat.completions.create(**payload)  # type: ignore

            return response.choices[0].message.content or ""

        except Exception as e:
            logger.error("Local LLM API error (%s): %s", self.base_url, e)
            raise

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
