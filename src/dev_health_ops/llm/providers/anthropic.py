"""
Anthropic LLM provider implementation.
"""

from __future__ import annotations

import logging

from dev_health_ops.llm.errors import classify_provider_error, call_with_retry

logger = logging.getLogger(__name__)


class AnthropicProvider:
    """
    Anthropic LLM provider using the messages API.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "claude-3-haiku-20240307",
        max_tokens: int = 4096,
        temperature: float = 0.3,
    ) -> None:
        """
        Initialize Anthropic provider.

        Args:
            api_key: Anthropic API key
            model: Model to use (default: claude-3-haiku for cost efficiency)
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature (lower = more deterministic)
        """
        self.api_key = api_key
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self._client: object | None = None

    def _get_client(self) -> object:
        """Lazy initialize Anthropic client."""
        if self._client is None:
            try:
                from anthropic import AsyncAnthropic

                self._client = AsyncAnthropic(api_key=self.api_key)
            except ImportError:
                raise ImportError(
                    "Anthropic package not installed. Install with: pip install anthropic"
                )
        return self._client

    async def _call(self, prompt: str) -> str:
        """Single attempt — called via call_with_retry."""
        client = self._get_client()

        is_json_prompt = (
            "Output schema" in (prompt or "")
            and '"subcategories"' in (prompt or "")
            and '"evidence_quotes"' in (prompt or "")
            and '"uncertainty"' in (prompt or "")
        )
        system = (
            "You are a JSON generator. Return a single JSON object only. "
            "Do not output markdown, code fences, comments, or extra text."
            if is_json_prompt
            else (
                "You are an assistant that explains precomputed work analytics. "
                "Use probabilistic language (appears, leans, suggests). "
                "Never use definitive language (is, was, detected, determined)."
            )
        )
        try:
            response = await client.messages.create(  # type: ignore
                model=self.model,
                max_tokens=self.max_tokens,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )

            if response.content and len(response.content) > 0:
                return response.content[0].text
            return ""
        except Exception as exc:
            raise classify_provider_error(
                exc, provider="anthropic", model=self.model
            ) from exc

    async def complete(self, prompt: str) -> str:
        """
        Generate a completion using Anthropic's API.

        Uses call_with_retry for uniform error handling across providers.

        Args:
            prompt: The prompt text to complete

        Returns:
            The generated completion text
        """
        return await call_with_retry(
            provider_name="anthropic",
            model=self.model,
            call=lambda: self._call(prompt),
            max_retries=1,
        )

    async def aclose(self) -> None:
        if self._client:
            await self._client.close()  # type: ignore
