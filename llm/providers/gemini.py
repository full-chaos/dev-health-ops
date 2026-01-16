"""
Gemini 3 LLM provider implementation.

Uses Google's OpenAI-compatible endpoint when configured.
"""

from __future__ import annotations

import os
from typing import Optional

from .local import LocalProvider

DEFAULT_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai"


class GeminiProvider(LocalProvider):
    """
    Gemini 3 provider via OpenAI-compatible endpoint.

    Configure via environment variables:
    - GEMINI_API_KEY: Your API key
    - GEMINI_MODEL: Model name (default: gemini-3)
    - GEMINI_BASE_URL: Optional endpoint override
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            api_key=api_key or os.getenv("GEMINI_API_KEY"),
            base_url=base_url or os.getenv("GEMINI_BASE_URL", DEFAULT_GEMINI_BASE_URL),
            model=model or os.getenv("GEMINI_MODEL", "gemini-3"),
            **kwargs,
        )
