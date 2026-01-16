"""
LLM Provider abstraction layer.

Provides a unified interface for LLM completion, supporting multiple backends.
"""

from __future__ import annotations

import os
from typing import Protocol, runtime_checkable, Optional


@runtime_checkable
class LLMProvider(Protocol):
    """Protocol for LLM providers."""

    async def complete(self, prompt: str) -> str:
        """
        Generate a completion for the given prompt.

        Args:
            prompt: The prompt text to complete

        Returns:
            The generated completion text
        """
        raise NotImplementedError()

    async def aclose(self) -> None:
        """Close the underlying client and release resources."""
        raise NotImplementedError()


def get_provider(name: str = "auto", model: Optional[str] = None) -> LLMProvider:
    """
    Get an LLM provider by name.

    Args:
        name: Provider name:
              - "auto": Detect from environment (OPENAI_API_KEY, ANTHROPIC_API_KEY,
                        GEMINI_API_KEY, LOCAL_LLM_BASE_URL, DASHSCOPE_API_KEY/QWEN_API_KEY,
                        OLLAMA_* or fall back to mock)
              - "openai": OpenAI API
              - "anthropic": Anthropic API
              - "gemini": Google Gemini 3 API (OpenAI-compatible)
              - "local": Generic OpenAI-compatible local server
              - "ollama": Ollama server (localhost:11434)
              - "lmstudio": LMStudio server (localhost:1234)
              - "qwen": Official Qwen / DashScope API
              - "qwen-local": Local Qwen (Ollama)
              - "qwen-lmstudio": LM Studio Qwen
              - "mock": Deterministic mock for testing
        model: Optional model name to override provider default.

    Returns:
        An LLMProvider instance

    Raises:
        ValueError: If the specified provider is not available
    """
    if name == "auto":
        # Check LLM_PROVIDER env var first
        env_name = os.getenv("LLM_PROVIDER")
        if env_name and env_name != "auto":
            name = env_name
        else:
            # Auto-detect based on other environment variables
            if os.getenv("OPENAI_API_KEY"):
                name = "openai"
            elif os.getenv("ANTHROPIC_API_KEY"):
                name = "anthropic"
            elif os.getenv("GEMINI_API_KEY"):
                name = "gemini"
            elif os.getenv("LOCAL_LLM_BASE_URL"):
                name = "local"
            elif os.getenv("DASHSCOPE_API_KEY") or os.getenv("QWEN_API_KEY"):
                name = "qwen"
            elif os.getenv("OLLAMA_MODEL") or os.getenv("OLLAMA_BASE_URL"):
                name = "ollama"
            else:
                # Fall back to mock for development/testing
                name = "mock"

    if model is None:
        model = os.getenv("LLM_MODEL")

    if name == "mock":
        from .mock import MockProvider

        return MockProvider()

    if name == "openai":
        from .openai import OpenAIProvider

        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("OPENAI_BASE_URL")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        return OpenAIProvider(
            api_key=api_key, base_url=base_url, model=model or "gpt-5-mini"
        )

    if name == "anthropic":
        from .anthropic import AnthropicProvider

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
        return AnthropicProvider(
            api_key=api_key, model=model or "claude-3-haiku-20240307"
        )

    if name == "gemini":
        from .gemini import GeminiProvider

        return GeminiProvider(model=model)

    if name == "local":
        from .local import LocalProvider

        return LocalProvider(model=model)

    if name == "ollama":
        from .local import OllamaProvider

        return OllamaProvider(model=model)

    if name == "lmstudio":
        if model and model.startswith("openai/gpt-oss"):
            from .local import LMStudioGPT5Provider

            return LMStudioGPT5Provider(model=model)

        from .local import LMStudioProvider

        return LMStudioProvider(model=model)

    if name == "qwen":
        from .qwen import QwenProvider

        return QwenProvider(model=model)

    if name == "qwen-local":
        from .qwen import QwenLocalProvider

        return QwenLocalProvider(model=model)

    if name == "qwen-lmstudio":
        from .qwen import QwenLMStudioProvider

        return QwenLMStudioProvider(model=model)

    raise ValueError(f"Unknown LLM provider: {name}")


__all__ = ["LLMProvider", "get_provider"]
