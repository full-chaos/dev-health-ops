"""
Qwen LLM provider implementation.

Supports:
- Official Qwen / DashScope API (OpenAI-compatible)
- Local Qwen (Ollama, etc.)
- LM Studio
"""

from __future__ import annotations

import os
from typing import Optional

from .local import LocalProvider, DEFAULT_ENDPOINTS

# Default DashScope (China) OpenAI-compatible endpoint.
# Users can override with DASHSCOPE_BASE_URL for international regions:
# - Singapore: https://dashscope-intl.aliyuncs.com/compatible-mode/v1
# - US (Virginia): https://dashscope-us.aliyuncs.com/compatible-mode/v1
DEFAULT_DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"


class QwenProvider(LocalProvider):
    """
    Official Qwen / DashScope provider via OpenAI-compatible endpoint.

    Configure via environment variables:
    - QWEN_API_KEY or DASHSCOPE_API_KEY: Your API key
    - QWEN_MODEL: Model name (default: qwen-plus)
    - DASHSCOPE_BASE_URL: Optional regional endpoint override
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            api_key=api_key
            or os.getenv("QWEN_API_KEY")
            or os.getenv("DASHSCOPE_API_KEY"),
            base_url=base_url
            or os.getenv("DASHSCOPE_BASE_URL", DEFAULT_DASHSCOPE_BASE_URL),
            model=model or os.getenv("QWEN_MODEL", "qwen-plus"),
            **kwargs,
        )


class QwenLocalProvider(LocalProvider):
    """
    Local Qwen provider (e.g., via Ollama).
    """

    def __init__(
        self,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            base_url=base_url
            or os.getenv("OLLAMA_BASE_URL", DEFAULT_ENDPOINTS["ollama"]),
            model=model or os.getenv("QWEN_LOCAL_MODEL", "qwen2.5:7b"),
            **kwargs,
        )


class QwenLMStudioProvider(LocalProvider):
    """
    LM Studio provider for Qwen models.
    """

    def __init__(
        self,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            base_url=base_url
            or os.getenv("LMSTUDIO_BASE_URL", DEFAULT_ENDPOINTS["lmstudio"]),
            model=model or os.getenv("LMSTUDIO_MODEL", "local-model"),
            **kwargs,
        )
