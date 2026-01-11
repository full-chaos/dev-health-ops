"""
OpenAI LLM provider implementation.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class OpenAIProvider:
    """
    OpenAI LLM provider using the chat completions API.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        max_tokens: int = 1024,
        temperature: float = 0.3,
    ) -> None:
        """
        Initialize OpenAI provider.

        Args:
            api_key: OpenAI API key
            model: Model to use (default: gpt-4o-mini for cost efficiency)
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature (lower = more deterministic)
        """
        self.api_key = api_key
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self._client: Optional[object] = None

    def _get_client(self) -> object:
        """Lazy initialize OpenAI client."""
        if self._client is None:
            try:
                from openai import AsyncOpenAI

                self._client = AsyncOpenAI(api_key=self.api_key)
            except ImportError:
                raise ImportError(
                    "OpenAI package not installed. Install with: pip install openai"
                )
        return self._client

    async def complete(self, prompt: str) -> str:
        """
        Generate a completion using OpenAI's API.

        Args:
            prompt: The prompt text to complete

        Returns:
            The generated completion text
        """
        client = self._get_client()

        try:
            # Use the chat completions API
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
            logger.error("OpenAI API error: %s", e)
            raise
