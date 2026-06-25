"""
Qwen LLM provider implementation.

Supports:
- Official Qwen / DashScope API (OpenAI-compatible)
- Local Qwen (Ollama, etc.)
- LM Studio
"""

from __future__ import annotations

import os

from .base import DEFAULT_MODEL_BY_PROVIDER
from .batch import (
    BatchCapability,
    BatchItemRequest,
    BatchItemResult,
    BatchJobState,
    BatchJobSubmission,
)
from .local import DEFAULT_ENDPOINTS, LocalProvider
from .openai import OpenAIGPTLegacyProvider, OpenAIProviderConfig

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

    provider_name = "qwen"

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        base_url: str | None = None,
        **kwargs,
    ) -> None:
        self._batch_impl: OpenAIGPTLegacyProvider | None = None
        super().__init__(
            api_key=api_key
            or os.getenv("QWEN_API_KEY")
            or os.getenv("DASHSCOPE_API_KEY"),
            base_url=base_url
            or os.getenv("DASHSCOPE_BASE_URL", DEFAULT_DASHSCOPE_BASE_URL),
            model=model or os.getenv("QWEN_MODEL", DEFAULT_MODEL_BY_PROVIDER["qwen"]),
            **kwargs,
        )

    def _get_batch_impl(self) -> OpenAIGPTLegacyProvider:
        if self._batch_impl is None:
            self._batch_impl = OpenAIGPTLegacyProvider(
                OpenAIProviderConfig(
                    api_key=self.api_key or "",
                    base_url=self.base_url,
                    model=self.model,
                    max_output_tokens=self.max_completion_tokens,
                    temperature=self.temperature,
                    validation_provider_name="qwen",
                )
            )
        return self._batch_impl

    def batch_capability(self, model: str | None = None) -> BatchCapability:
        return self._get_batch_impl().batch_capability(model or self.model)

    async def submit_batch(self, items: list[BatchItemRequest]) -> BatchJobSubmission:
        return await self._get_batch_impl().submit_batch(items)

    async def poll_batch(self, provider_job_id: str) -> BatchJobState:
        return await self._get_batch_impl().poll_batch(provider_job_id)

    async def fetch_batch_results(self, provider_job_id: str) -> list[BatchItemResult]:
        return await self._get_batch_impl().fetch_batch_results(provider_job_id)

    async def cancel_batch(self, provider_job_id: str) -> None:
        await self._get_batch_impl().cancel_batch(provider_job_id)


class QwenLocalProvider(LocalProvider):
    """
    Local Qwen provider (e.g., via Ollama).
    """

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
            or os.getenv("QWEN_LOCAL_MODEL", DEFAULT_MODEL_BY_PROVIDER["qwen-local"]),
            **kwargs,
        )


class QwenLMStudioProvider(LocalProvider):
    """
    LM Studio provider for Qwen models.
    """

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            base_url=base_url
            or os.getenv("LMSTUDIO_BASE_URL", DEFAULT_ENDPOINTS["lmstudio"]),
            model=model
            or os.getenv("LMSTUDIO_MODEL", DEFAULT_MODEL_BY_PROVIDER["qwen-lmstudio"]),
            **kwargs,
        )
