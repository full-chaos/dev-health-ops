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
        pass

    async def aclose(self) -> None:
        """Close the underlying client and release resources."""
        pass


def get_provider(name: str = "auto", model: Optional[str] = None) -> LLMProvider:
    """
    Get an LLM provider by name.

    Args:
        name: Provider name:
              - "auto": Detect from environment (OPENAI_API_KEY, ANTHROPIC_API_KEY,
                        LOCAL_LLM_BASE_URL, or fall back to mock)
              - "openai": OpenAI API
              - "anthropic": Anthropic API
              - "local": Generic OpenAI-compatible local server
              - "ollama": Ollama server (localhost:11434)
              - "lmstudio": LMStudio server (localhost:1234)
              - "mock": Deterministic mock for testing
        model: Optional model name to override provider default.

    Returns:
        An LLMProvider instance

    Raises:
        ValueError: If the specified provider is not available
    """
    if name == "auto":
        # Auto-detect based on environment variables
        if os.getenv("OPENAI_API_KEY"):
            name = "openai"
        elif os.getenv("ANTHROPIC_API_KEY"):
            name = "anthropic"
        elif os.getenv("LOCAL_LLM_BASE_URL"):
            name = "local"
        elif os.getenv("OLLAMA_MODEL") or os.getenv("OLLAMA_BASE_URL"):
            name = "ollama"
        else:
            # Fall back to mock for development/testing
            name = "mock"

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

    if name == "local":
        from .local import LocalProvider

        return LocalProvider(model=model)

    if name == "ollama":
        from .local import OllamaProvider

        return OllamaProvider(model=model)

    if name == "lmstudio":
        from .local import LMStudioProvider

        return LMStudioProvider(model=model)

    raise ValueError(f"Unknown LLM provider: {name}")


__all__ = ["LLMProvider", "get_provider"]
