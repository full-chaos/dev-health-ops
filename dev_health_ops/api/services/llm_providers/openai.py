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
        model: str = "gpt-5-mini",
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

    def _token_param_name(self) -> str:
        """
        Some OpenAI models (notably reasoning-style families) require
        `max_completion_tokens` instead of `max_tokens`.
        """
        model = (self.model or "").strip()
        if not model:
            return "max_tokens"
        if model.startswith(("o1", "o3", "gpt-5")):
            return "max_completion_tokens"
        return "max_tokens"

    def _supports_temperature(self) -> bool:
        """
        Some models only support the default temperature (1) and reject any
        explicitly set non-default value.
        """
        model = (self.model or "").strip()
        if not model:
            return True
        if model.startswith(("o1", "o3", "gpt-5")):
            return False
        return True

    def _is_json_schema_prompt(self, prompt: str) -> bool:
        text = prompt or ""
        return (
            "Output schema" in text
            and '"subcategories"' in text
            and '"evidence_quotes"' in text
            and '"uncertainty"' in text
        )

    def _system_message(self, prompt: str) -> str:
        if self._is_json_schema_prompt(prompt):
            return (
                "You are a JSON generator.\n"
                "Return a single JSON object only.\n"
                "Do not output markdown, code fences, comments, or extra text."
            )
        return (
            "You are an assistant that explains precomputed work analytics. "
            "Use probabilistic language (appears, leans, suggests). "
            "Never use definitive language (is, was, detected, determined)."
        )

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
            token_param = self._token_param_name()
            token_kwargs = {token_param: self.max_tokens}
            temperature_kwargs = (
                {"temperature": self.temperature}
                if self._supports_temperature()
                else {}
            )
            system_message = self._system_message(prompt)
            # Use the chat completions API
            response = await client.chat.completions.create(  # type: ignore
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": system_message,
                    },
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                **token_kwargs,
                **temperature_kwargs,
            )

            return response.choices[0].message.content or ""

        except Exception as e:
            logger.error("OpenAI API error: %s", e)
            raise
